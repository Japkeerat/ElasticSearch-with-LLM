import os
import logging
from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)


class ElasticsearchConnection:
    """Shared Elasticsearch connection for all tools."""

    _instance = None
    _es_client = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_client(self) -> Elasticsearch:
        """Get or create Elasticsearch client."""
        if self._es_client is None:
            self._es_client = self._connect_to_elasticsearch()
        return self._es_client

    def _connect_to_elasticsearch(self) -> Elasticsearch:
        """
        Create Elasticsearch connection using environment variables.

        Returns:
            Elasticsearch client instance
        """
        es_host = os.getenv("ES_HOST", "http://localhost:9200")
        es_api_key = os.getenv("ES_API_KEY")

        if es_api_key:
            es_client = Elasticsearch(es_host, api_key=es_api_key)
        else:
            # For local development without API key
            es_client = Elasticsearch(es_host)

        logger.info(f"Connected to Elasticsearch at {es_host}")
        return es_client
