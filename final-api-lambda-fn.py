import json
from pymongo import MongoClient
from datetime import datetime
import os

def lambda_handler(event, context):
    try:
        client = MongoClient(
            "mongodb://summarizedtext:ccproject2025@summarized-text.crgm2442ss7l.us-west-2.docdb.amazonaws.com:27017/?tls=true&tlsAllowInvalidCertificates=true",
            tls=True,
            tlsAllowInvalidCertificates=True,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=5000,
            retryWrites=False
        )

        db = client['TranscriptionDB']
        collection = db['Summaries']

        # Fetch all published summaries
        published_docs = collection.find({"published": True}).sort("publishedAt", -1).limit(1)
        
        summaries = []
        for doc in published_docs:
            summaries.append({
                "id": str(doc["_id"]),
                "summaryText": doc.get("summaryText", ""),
                "publishedAt": doc.get("publishedAt", "")
            })

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(summaries)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)})
        }
