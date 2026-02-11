import json
import uuid
import boto3
import os
import urllib.parse
from io import StringIO

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
textract = boto3.client('textract')

QUEUE_URL = os.environ['SQS_QUEUE_URL']

def smart_flush_to_sqs(buffer, metadata):
    """Sends exactly what is in the buffer (up to 10) to SQS with batch safety"""
    if not buffer:
        return
    
    entries = []
    for chunk_text in buffer:
        # Merge metadata with the specific text chunk
        body = {**metadata, "text": chunk_text, "chunk_id": str(uuid.uuid4())[:8]}
        
        # SQS Batch limit check (256KB total)
        encoded_body = json.dumps(body)
        if len(encoded_body.encode('utf-8')) > 250000:
            print(f"Warning: Chunk in {metadata['filename']} too large, truncating.")
            body["text"] = chunk_text[:100000]
            encoded_body = json.dumps(body)

        entries.append({
            'Id': str(uuid.uuid4()),
            'MessageBody': encoded_body
        })
    
    try:
        sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=entries)
    except Exception as e:
        print(f"SQS Batch Error: {e}")

def get_metadata_from_key(key):
    """Parses raw/user_id/subject_id/file_id/filename"""
    parts = key.split('/')
    if len(parts) >= 5:
        return {
            "user_id": parts[1],
            "subject_id": parts[2],
            "file_id": parts[3],
            "filename": parts[4]
        }
    return {"user_id": "unknown", "subject_id": "unknown", "file_id": "unknown", "filename": parts[-1]}

def smart_recursive_splitter(text, max_size=800, overlap=100):
    """Splits text by Paragraphs -> Sentences -> Words to keep context alive."""
    if not text: return []
    
    paragraphs = text.split("\n\n")
    chunks = []
    current_chunk = ""

    for para in paragraphs:
        if len(current_chunk) + len(para) <= max_size:
            current_chunk += para + "\n\n"
        else:
            if current_chunk:
                chunks.append(current_chunk.strip())
            
            # If paragraph is a monster, break it into sentences
            if len(para) > max_size:
                sentences = para.replace("? ", "?. ").replace("! ", "!. ").split(". ")
                sub_chunk = ""
                for sent in sentences:
                    if len(sub_chunk) + len(sent) <= max_size:
                        sub_chunk += sent + ". "
                    else:
                        if sub_chunk: chunks.append(sub_chunk.strip())
                        sub_chunk = sent + ". "
                current_chunk = sub_chunk
            else:
                current_chunk = para + "\n\n"

    if current_chunk:
        chunks.append(current_chunk.strip())
    return chunks

def lambda_handler(event, context):
    for record in event['Records']:
        raw_key = record['s3']['object']['key']
        key = urllib.parse.unquote_plus(raw_key)
        bucket = record['s3']['bucket']['name']
        ext = key.split('.')[-1].lower()
        
        metadata = get_metadata_from_key(key)
        metadata["file_type"] = ext
        
        extracted_text = ""

        # --- SMART EXTRACTION ---
        if ext in ['pdf', 'png', 'jpg', 'jpeg']:
            response = textract.detect_document_text(
                Document={'S3Object': {'Bucket': bucket, 'Name': key}}
            )
            extracted_text = "\n".join([b['Text'] for b in response['Blocks'] if b['BlockType'] == 'LINE'])

        elif ext in ['txt', 'csv']:
            # Stream the file content
            response = s3.get_object(Bucket=bucket, Key=key)
            extracted_text = response['Body'].read().decode('utf-8')

        else:
            print(f"Skipping unsupported type: {ext}")
            continue

        # --- SMART CHUNKING & BATCHING ---
        # We run the recursive splitter on the full extracted text
        all_chunks = smart_recursive_splitter(extracted_text)
        
        sqs_buffer = []
        for chunk in all_chunks:
            sqs_buffer.append(chunk)
            # Flush every 10 chunks (Smart Batching)
            if len(sqs_buffer) == 10:
                smart_flush_to_sqs(sqs_buffer, metadata)
                sqs_buffer = []

        # Final flush for remaining chunks (<10)
        if sqs_buffer:
            smart_flush_to_sqs(sqs_buffer, metadata)

    return {"status": "Complete", "total_records": len(event['Records'])}