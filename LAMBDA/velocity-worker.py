import json
import os
import boto3
from pinecone import Pinecone

# Initialize clients outside the handler for reuse (Fast execution)
bedrock = boto3.client('bedrock-runtime')
pc = Pinecone(api_key=os.environ['PINECONE_API_KEY'])
index = pc.Index(os.environ['PINECONE_INDEX_NAME'])

def lambda_handler(event, context):
    # event['Records'] contains 1 to 10 SQS messages
    vectors_to_upsert = []
    
    for record in event['Records']:
        try:
            # Parse SQS Message
            body = json.loads(record['body'])
            text = body['text']
            metadata = {
                "user_id": body['user_id'],
                "subject_id": body['subject_id'],
                "file_id": body['file_id'],
                "text": text[:1000] # Pinecone metadata limit is 40KB; don't store huge text
            }
            
            # 1. Get Embedding from Bedrock
            bedrock_payload = json.dumps({
                "inputText": text,
                "dimensions": 1024,
                "normalize": True
            })
            
            response = bedrock.invoke_model(
                body=bedrock_payload,
                modelId=os.environ['BEDROCK_MODEL_ID'],
                accept='application/json',
                contentType='application/json'
            )
            
            response_body = json.loads(response.get('body').read())
            embedding = response_body.get('embedding')
            
            # 2. Prepare Vector Object
            vector_id = f"{body['file_id']}#{body['chunk_index']}"
            vectors_to_upsert.append({
                "id": vector_id,
                "values": embedding,
                "metadata": metadata
            })
            
        except Exception as e:
            print(f"Error processing record {record['messageId']}: {str(e)}")
            # In production, you'd report this as a batch failure
    
    # 3. Batch Upsert to Pinecone
    if vectors_to_upsert:
        index.upsert(vectors=vectors_to_upsert)
        print(f"Successfully upserted {len(vectors_to_upsert)} vectors.")

    return {"statusCode": 200}