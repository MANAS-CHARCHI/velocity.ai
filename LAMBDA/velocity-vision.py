import boto3, base64, json, os

bedrock = boto3.client('bedrock-runtime')
sqs = boto3.client('sqs')
s3 = boto3.client('s3')
QUEUE_URL = os.environ['WORKER_SQS_URL']

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    meta = s3.head_object(Bucket=bucket, Key=key)['Metadata']

    # 1. Image to Base64
    img_bytes = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
    encoded = base64.b64encode(img_bytes).decode('utf-8')

    # 2. Call Bedrock
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31", "max_tokens": 300,
        "messages": [{"role": "user", "content": [
            {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": encoded}},
            {"type": "text", "text": "Describe this diagram or figure concisely for a knowledge database."}
        ]}]
    })
    
    response = bedrock.invoke_model(modelId="anthropic.claude-3-haiku-20240307-v1:0", body=body)
    desc = json.loads(response.get('body').read())['content'][0]['text']

    # 3. Send to SQS
    sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps({
        "type": "image_description", "content": desc, 
        "image_url": f"s3://{bucket}/{key}", "metadata": meta
    }))