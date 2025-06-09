import os

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from config import INDEX_NAME, INDEX_MAPPING

load_dotenv()

es = Elasticsearch(os.environ.get("ES_HOST"), api_key=os.environ.get("ES_API_KEY"))

def setup_index():
    if not es.indices.exists(index=INDEX_NAME):
        es.indices.create(index=INDEX_NAME, body=INDEX_MAPPING)

def bulk_insert(logs: list):
    actions = [
        {
            "_index": INDEX_NAME,
            "_source": log
        }
        for log in logs
    ]
    bulk(es, actions)
