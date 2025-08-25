import json
import logging
from typing import Dict, Any
from llm_es_agent.tools.connection import ElasticsearchConnection

logger = logging.getLogger(__name__)


class QueryExecutionTools:
    """Minimal tools for executing queries against Elasticsearch."""

    def __init__(self):
        """Initialize with shared Elasticsearch connection."""
        self.es = ElasticsearchConnection().get_client()

    def execute_query(self, query_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a validated query from the query generation agent.

        Args:
            query_data: Complete query data from query generation agent including:
                - generated_query: The query DSL and metadata
                - target_index: Index name
                - validation: Validation results

        Returns:
            Raw execution results for LLM analysis
        """
        try:
            # Extract components from query data
            if "generated_query" not in query_data or not query_data["generated_query"]:
                return {"error": "No generated query found in query data"}

            generated_query = query_data["generated_query"]
            target_index = query_data.get("target_index")
            validation = query_data.get("validation", {})

            # Check if query is ready for execution
            if not validation.get("ready_for_execution", False):
                return {
                    "error": "Query is not ready for execution",
                    "validation_issues": validation,
                }

            # Extract the actual query DSL
            query_dsl = generated_query.get("query_dsl")
            if not query_dsl:
                return {"error": "No query DSL found in generated query"}

            # Execute the query
            result = self._execute_elasticsearch_query(target_index, query_dsl)

            # Add minimal metadata for context
            if "error" not in result:
                result["query_metadata"] = {
                    "query_type": generated_query.get("query_type"),
                    "target_fields": generated_query.get("target_fields", []),
                    "complexity": generated_query.get("estimated_complexity"),
                    "target_index": target_index,
                }

            return result

        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return {"error": f"Failed to execute query: {str(e)}"}

    def _execute_elasticsearch_query(
        self, index_name: str, query: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute a query against Elasticsearch and return raw results.

        Args:
            index_name: Name of the index to search
            query: Elasticsearch query DSL dictionary

        Returns:
            Raw search results from Elasticsearch
        """
        try:
            # Validate that index exists
            if not self.es.indices.exists(index=index_name):
                return {"error": f"Index '{index_name}' does not exist"}

            # Security check - ensure this is a read-only operation
            if not self._is_read_only_query(query):
                return {"error": "Only read-only queries are allowed"}

            # Execute the search
            response = self.es.search(index=index_name, body=query)

            # Return clean, minimal results for LLM analysis
            return {
                "total_hits": response["hits"]["total"]["value"],
                "max_score": response["hits"]["max_score"],
                "documents": [
                    {
                        "id": hit.get("_id"),
                        "score": hit["_score"],
                        "source": hit["_source"],
                    }
                    for hit in response["hits"]["hits"]
                ],
                "aggregations": response.get("aggregations", {}),
                "took_ms": response["took"],
                "timed_out": response.get("timed_out", False),
            }

        except Exception as e:
            logger.error(f"Error executing search query on {index_name}: {str(e)}")
            return {"error": f"Failed to execute search: {str(e)}"}

    def _is_read_only_query(self, query: Dict[str, Any]) -> bool:
        """
        Validate that the query is read-only and doesn't contain any write operations.

        Args:
            query: Elasticsearch query DSL

        Returns:
            True if query is read-only, False otherwise
        """
        # Convert query to string to check for dangerous operations
        query_str = json.dumps(query).lower()

        # Check for write operations
        write_operations = [
            "update",
            "delete",
            "create",
            "index",
            "bulk",
            "_update",
            "_delete",
            "_create",
            "script",
        ]

        for operation in write_operations:
            if operation in query_str:
                logger.warning(
                    f"Potentially unsafe operation '{operation}' detected in query"
                )
                return False

        return True
