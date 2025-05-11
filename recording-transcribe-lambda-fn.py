import json
import boto3
import urllib.parse
import os
from datetime import datetime

transcribe = boto3.client('transcribe')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Get bucket and file details from S3 event
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        
        # Validate we're processing the correct bucket
        if source_bucket != 'recording-bucket-cc2025':
            print(f"Unexpected bucket: {source_bucket}. Exiting.")
            return
            
        # Generate unique job name (Transcribe requirement)
        file_name = os.path.basename(object_key)
        job_name = f"transcribe-{file_name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        # Media file URI for Transcribe
        media_uri = f"s3://{source_bucket}/{object_key}"
        
        # Supported audio formats - adjust as needed
        media_format = file_name.split('.')[-1]
        if media_format not in ['m4a', 'mp3', 'mp4', 'wav', 'flac', 'ogg', 'amr', 'webm']:
            media_format = 'm4a'  # default fallback
        
        # Start transcription job
        response = transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            LanguageCode='en-US',  # Change language code as needed
            MediaFormat=media_format,
            Media={
                'MediaFileUri': media_uri
            },
            OutputBucketName='transcription-output-bucket-cc2025',
            Settings={
                'ShowSpeakerLabels': True,
                'MaxSpeakerLabels': 2  # Adjust based on expected speakers
            }
        )
        
        print(f"Started transcription job: {job_name}")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Transcription job started successfully',
                'jobName': job_name,
                'mediaUri': media_uri
            })
        }
        
    except Exception as e:
        print(f"Error processing file {object_key}: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to start transcription job',
                'error': str(e),
                'inputEvent': event
            })
        }
