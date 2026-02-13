import boto3, os, fitz, json

textract = boto3.client('textract')
s3 = boto3.client('s3')

# Environment variables for SNS and Role
SNS_TOPIC_ARN = os.environ['TEXTRACT_SNS_TOPIC']
ROLE_ARN = os.environ['TEXTRACT_ROLE_ARN']
MANAGER_LAMBDA = os.environ['MANAGER_LAMBDA_NAME']

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # 1. Fetch Passport Metadata (user_id, subject_id, file_id)
    head = s3.head_object(Bucket=bucket, Key=key)
    meta = head['Metadata'] 

    # 2. Check page count
    local_path = f"/tmp/{os.path.basename(key)}"
    s3.download_file(bucket, key, local_path)
    doc = fitz.open(local_path)
    page_count = len(doc)
    doc.close()

    if page_count == 1:
        # Sync Path: Call Textract and pass result directly to Manager
        response = textract.analyze_document(
            Document={'S3Object': {'Bucket': bucket, 'Name': key}},
            FeatureTypes=["LAYOUT"]
        )
        boto3.client('lambda').invoke(
            FunctionName=MANAGER_LAMBDA,
            InvocationType='Event',
            Payload=json.dumps({"sync_result": response, "bucket": bucket, "key": key, "metadata": meta})
        )
    else:
        # Async Path: Textract will notify SNS when finished
        textract.start_document_analysis(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}},
            FeatureTypes=["LAYOUT"],
            NotificationChannel={'SNSTopicArn': SNS_TOPIC_ARN, 'RoleArn': ROLE_ARN}
        )
    
    return {"status": "success", "mode": "async" if page_count > 1 else "sync"}