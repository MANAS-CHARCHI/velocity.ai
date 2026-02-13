import json
import os
import boto3
from pinecone import Pinecone

bedrock = boto3.client('bedrock-runtime')
s3 = boto3.client('s3')
pc = Pinecone(api_key=os.environ['PINECONE_API_KEY'])
index = pc.Index(os.environ['PINECONE_INDEX_NAME'])

VAULT_BUCKET = os.environ['VAULT_BUCKET']
METADATA_BUCKET = os.environ['METADATA_BUCKET']

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            body = json.loads(record['body'])
            content = body['content']
            meta = body['metadata']
            file_id = meta['file_id']
            part_num = body['part_num']
            total_parts = body['total_parts']

            # 1. Pinecone Vector
            res = bedrock.invoke_model(
                body=json.dumps({"inputText": content, "dimensions": 1024, "normalize": True}),
                modelId='amazon.titan-embed-text-v2:0', accept='application/json', contentType='application/json'
            )
            embedding = json.loads(res.get('body').read()).get('embedding')
            
            index.upsert(vectors=[{
                "id": f"{file_id}#{part_num}",
                "values": embedding,
                "metadata": {**meta, "text": content[:1000]}
            }])

            # 2. Save Part to Temp Folder (Zero overwrite)
            temp_key = f"temp/{file_id}/part_{part_num:05}.txt"
            s3.put_object(Bucket=VAULT_BUCKET, Key=temp_key, Body=content)

            # 3. If Last Part, Assemble File
            if part_num == total_parts:
                assemble_final_file(file_id, meta)

        except Exception as e:
            print(f"Error: {e}")

def assemble_final_file(file_id, meta):
    # 1. List and sort all parts
    prefix = f"temp/{file_id}/"
    objs = s3.list_objects_v2(Bucket=VAULT_BUCKET, Prefix=prefix)['Contents']
    sorted_objs = sorted(objs, key=lambda x: x['Key'])
    
    # 2. Join text
    full_text = []
    for obj in sorted_objs:
        txt = s3.get_object(Bucket=VAULT_BUCKET, Key=obj['Key'])['Body'].read().decode('utf-8')
        full_text.append(txt)
    
    # 3. Final Vault Write (ONE write only)
    vault_key = f"vault/{meta['user_id']}/{meta['subject_id']}/{file_id}.txt"
    s3.put_object(Bucket=VAULT_BUCKET, Key=vault_key, Body="\n\n".join(full_text))
    
    # 4. Cleanup Temp
    for obj in sorted_objs:
        s3.delete_object(Bucket=VAULT_BUCKET, Key=obj['Key'])