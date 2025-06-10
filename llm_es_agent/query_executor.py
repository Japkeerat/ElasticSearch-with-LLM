# query_executor.py
from elasticsearch import Elasticsearch
import json, os
from dotenv import load_dotenv

load_dotenv()

es = Elasticsearch(os.getenv("ES_HOST"),
                   api_key=os.getenv("ES_API_KEY"),)
INDEX = "audit-logs"

def run_query(query: dict):
    response = es.search(index=INDEX, body=query)
    return response
