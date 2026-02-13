import boto3
import json
import os
import csv
import io

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
textract = boto3.client('textract')
lambda_client = boto3.client('lambda')

CROPPER_LAMBDA = os.environ['CROPPER_LAMBDA_NAME']
WORKER_QUEUE_URL = os.environ['WORKER_SQS_URL']
SNS_TOPIC_ARN = os.environ['TEXTRACT_SNS_TOPIC']
ROLE_ARN = os.environ['TEXTRACT_ROLE_ARN']

def lambda_handler(event, context):
    # 1. Handle Textract Callback
    if 'Records' in event and 'Sns' in event['Records'][0]:
        return handle_textract_callback(event)
    
    # 2. Handle Direct S3 Uploads
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    ext = key.split('.')[-1].lower()
    meta = s3.head_object(Bucket=bucket, Key=key)['Metadata']

    if ext == 'pdf':
        textract.start_document_analysis(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}},
            FeatureTypes=["LAYOUT"],
            NotificationChannel={'SNSTopicArn': SNS_TOPIC_ARN, 'RoleArn': ROLE_ARN}
        )

    elif ext == 'csv':
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj['Body'].read().decode('utf-8')
        reader = list(csv.DictReader(io.StringIO(content)))
        
        chunk_size = 20
        chunks = [reader[i:i + chunk_size] for i in range(0, len(reader), chunk_size)]
        total_parts = len(chunks)
        
        for idx, chunk in enumerate(chunks):
            row_texts = [" | ".join([f"{col}: {val}" for col, val in r.items() if val]) for r in chunk]
            send_to_worker(row_texts, meta, key, "csv", idx + 1, total_parts)
            
    elif ext == 'txt':
        obj = s3.get_object(Bucket=bucket, Key=key)
        text = obj['Body'].read().decode('utf-8')
        send_to_worker([text], meta, key, "text", 1, 1)

    return {"status": "success"}

def send_to_worker(content_list, meta, source_key, data_type, part_num, total_parts):
    combined_content = "\n".join(content_list)
    sqs.send_message(
        QueueUrl=WORKER_QUEUE_URL,
        MessageBody=json.dumps({
            "type": data_type,
            "content": combined_content,
            "part_num": part_num,
            "total_parts": total_parts,
            "metadata": {**meta, "source_file": source_key}
        })
    )

def handle_textract_callback(event):
    msg = json.loads(event['Records'][0]['Sns']['Message'])
    job_id, bucket, key = msg['JobId'], msg['DocumentLocation']['S3Bucket'], msg['DocumentLocation']['S3ObjectName']
    meta = s3.head_object(Bucket=bucket, Key=key)['Metadata']
    response = textract.get_document_analysis(JobId=job_id)
    
    # Aggregate LINEs by Page
    pages = {}
    for block in response['Blocks']:
        if block['BlockType'] == 'LAYOUT_FIGURE':
            lambda_client.invoke(FunctionName=CROPPER_LAMBDA, InvocationType='Event',
                                Payload=json.dumps({"bucket": bucket, "key": key, "metadata": meta, 
                                                   "bbox": block['Geometry']['BoundingBox'], "page": block.get('Page', 1), "id": block['Id']}))
        
        elif block['BlockType'] == 'LINE':
            p_num = block.get('Page', 1)
            pages.setdefault(p_num, []).append(block['Text'])

    total_pages = len(pages)
    for p_num, lines in pages.items():
        send_to_worker(lines, meta, key, "pdf_page", p_num, total_pages)