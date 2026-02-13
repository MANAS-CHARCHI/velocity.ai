import boto3, fitz, os

s3 = boto3.client('s3')
IMAGE_BUCKET = os.environ['IMAGE_BUCKET']

def lambda_handler(event, context):
    # 1. Download PDF
    local_pdf = f"/tmp/input.pdf"
    s3.download_file(event['bucket'], event['key'], local_pdf)
    
    # 2. Crop logic
    doc = fitz.open(local_pdf)
    page = doc[event['page'] - 1]
    bbox = event['bbox']
    rect = page.rect
    
    crop_rect = fitz.Rect(
        bbox['Left'] * rect.width, bbox['Top'] * rect.height,
        (bbox['Left'] + bbox['Width']) * rect.width, (bbox['Top'] + bbox['Height']) * rect.height
    )
    
    pix = page.get_pixmap(clip=crop_rect)
    img_key = f"crops/{event['id']}_{os.path.basename(event['key'])}.jpg"
    
    # 3. Save Image (This triggers Vision Lambda automatically)
    s3.put_object(
        Bucket=IMAGE_BUCKET, Key=img_key, Body=pix.tobytes("jpg"),
        Metadata=event['metadata'] # Carry user_id, subject_id, etc.
    )
    doc.close()