import json
import boto3
import urllib.parse
import os
from pymongo import MongoClient
from botocore.exceptions import ClientError
from pymongo.errors import PyMongoError
from datetime import datetime

# Initialize clients with timeouts and connection pooling
s3_client = boto3.client('s3', config=boto3.session.Config(
    connect_timeout=10,
    read_timeout=30,
    retries={'max_attempts': 3}
))
comprehend_client = boto3.client('comprehend')

# DocumentDB connection - initialized outside handler for reuse
documentdb_client = None

def get_documentdb_connection():
    global documentdb_client
    if not documentdb_client:
        documentdb_client = MongoClient(
            "mongodb://summarizedtext:ccproject2025@summarized-text.crgm2442ss7l.us-west-2.docdb.amazonaws.com:27017/?tls=true&tlsAllowInvalidCertificates=true",
            tls=True,
            tlsAllowInvalidCertificates=True,
            connectTimeoutMS=10000,
            socketTimeoutMS=30000,
            serverSelectionTimeoutMS=10000,
            maxPoolSize=5,
            retryWrites=False
        )
    return documentdb_client

def store_in_documentdb(document):
    """Store document in DocumentDB with retry logic"""
    try:
        client = get_documentdb_connection()
        db = client['TranscriptionDB']
        collection = db['Summaries']
        
        # Test connection first
        client.admin.command('ping')
        
        result = collection.insert_one(document)
        return str(result.inserted_id)
    except PyMongoError as e:
        print(f"DocumentDB error: {str(e)}")
        raise

def lambda_handler(event, context):
    try:
        print("Lambda execution started. Event:", json.dumps(event, indent=2))
        
        # Determine if this is an S3 trigger or API Gateway request
        if 'Records' in event:  # S3 Trigger
            # 1. Extract S3 bucket and object key
            bucket_name = event['Records'][0]['s3']['bucket']['name']
            object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
            print(f"Processing file: s3://{bucket_name}/{object_key}")
            
            # 2. Read and process file
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            transcript_data = json.loads(response['Body'].read().decode('utf-8'))
            transcript_text = transcript_data['results']['transcripts'][0]['transcript'][:5000]
            
            # 3. Generate summary with Comprehend
            syntax_response = comprehend_client.detect_syntax(
                Text=transcript_text,
                LanguageCode='en'
            )
            keywords = [
                token['Text'] for token in syntax_response['SyntaxTokens'] 
                if token['PartOfSpeech']['Tag'] in ['NOUN', 'PROPN']
            ]
            summary_text = ' '.join(keywords[:50]) + ' ...'
            
            # 4. Store in DocumentDB
            job_name = os.path.basename(object_key).replace('.json', '')
            document = {
                "jobName": job_name,
                "summaryText": summary_text,
                "sourceFile": f"s3://{bucket_name}/{object_key}",
                "processedAt": datetime.utcnow().isoformat(),
                "status": "COMPLETED"
            }
            
            summary_id = store_in_documentdb(document)
            print(f"✅ Document inserted with ID: {summary_id}")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Successfully processed',
                    'documentId': summary_id,
                    'summary': summary_text
                })
            }
            
        else:  # API Gateway request (GET /Summary)
            # Handle API requests to retrieve summaries
            print("Handling API Gateway request")
            
            # Extract query parameters or path parameters
            query_params = event.get('queryStringParameters', {})
            path_params = event.get('pathParameters', {})
            
            # Determine if requesting specific summary or list
            if path_params and 'summary_id' in path_params:
                # Get single summary by ID
                summary_id = path_params['summary_id']
                client = get_documentdb_connection()
                collection = client['TranscriptionDB']['Summaries']
                
                document = collection.find_one({"_id": summary_id})
                if not document:
                    return {
                        'statusCode': 404,
                        'body': json.dumps({'error': 'Summary not found'})
                    }
                    
                return {
                    'statusCode': 200,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'summary': document['summaryText'],
                        'source': document['sourceFile'],
                        'processedAt': document['processedAt'],
                        'status': document.get('status', 'UNKNOWN')
                    })
                }
            else:
                # List all summaries (with optional filters)
                client = get_documentdb_connection()
                collection = client['TranscriptionDB']['Summaries']
                
                # Build query from query parameters
                query = {}
                if 'status' in query_params:
                    query['status'] = query_params['status']
                
                documents = collection.find(query).limit(100)
                
                summaries = []
                for doc in documents:
                    summaries.append({
                        'id': str(doc['_id']),
                        'jobName': doc['jobName'],
                        'processedAt': doc['processedAt'],
                        'status': doc.get('status', 'UNKNOWN')
                    })
                
                return {
                    'statusCode': 200,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'count': len(summaries),
                        'summaries': summaries
                    })
                }

    except Exception as e:
        print(f"Fatal error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': str(e),
                'remaining_time': context.get_remaining_time_in_millis()
            })
        }
