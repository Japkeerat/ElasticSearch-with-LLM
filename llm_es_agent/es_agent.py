import os
import json
import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
from dotenv import load_dotenv

from elasticsearch import Elasticsearch
from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm
from google.adk.tools import Tool

logger = logging.getLogger(__name__)


class ElasticsearchTools:
    """Tools for interacting with Elasticsearch."""

    def __init__(self):
        """Initialize Elasticsearch connection."""
        self.es = self._connect_to_elasticsearch()

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

    def list_indices(self) -> Dict[str, Any]:
        """
        Get list of all available Elasticsearch indices.

        Returns:
            Dictionary containing indices information
        """
        try:
            # Get indices with stats
            indices_response = self.es.cat.indices(
                format="json", h="index,docs.count,store.size"
            )

            if not indices_response:
                return {"indices": [], "total_count": 0}

            indices_info = []
            for index in indices_response:
                indices_info.append(
                    {
                        "name": index.get("index", ""),
                        "document_count": index.get("docs.count", "0"),
                        "store_size": index.get("store.size", "0b"),
                    }
                )

            return {"indices": indices_info, "total_count": len(indices_info)}

        except Exception as e:
            logger.error(f"Error listing indices: {str(e)}")
            return {"error": f"Failed to list indices: {str(e)}"}

    def get_index_mapping(self, index_name: str) -> Dict[str, Any]:
        """
        Get the mapping/schema for a specific index.

        Args:
            index_name: Name of the Elasticsearch index

        Returns:
            Dictionary containing the index mapping
        """
        try:
            if not self.es.indices.exists(index=index_name):
                return {"error": f"Index '{index_name}' does not exist"}

            mapping = self.es.indices.get_mapping(index=index_name)

            # Extract and simplify the mapping structure
            index_mapping = mapping.get(index_name, {})
            mappings = index_mapping.get("mappings", {})
            properties = mappings.get("properties", {})

            # Create a simplified, human-readable schema
            schema = self._simplify_mapping(properties)

            return {
                "index": index_name,
                "schema": schema,
                "properties_count": len(properties),
            }

        except Exception as e:
            logger.error(f"Error getting mapping for index {index_name}: {str(e)}")
            return {
                "error": f"Failed to get mapping for index '{index_name}': {str(e)}"
            }

    def _simplify_mapping(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        Simplify Elasticsearch mapping structure for better readability.

        Args:
            properties: The properties section of an ES mapping

        Returns:
            Simplified schema structure
        """
        simplified = {}

        for field_name, field_config in properties.items():
            field_type = field_config.get("type", "unknown")

            # Handle nested objects
            if field_type == "object" and "properties" in field_config:
                simplified[field_name] = {
                    "type": "object",
                    "properties": self._simplify_mapping(field_config["properties"]),
                }
            elif field_type == "nested" and "properties" in field_config:
                simplified[field_name] = {
                    "type": "nested",
                    "properties": self._simplify_mapping(field_config["properties"]),
                }
            else:
                # Handle simple types
                field_info = {"type": field_type}

                # Add additional useful information
                if "fields" in field_config:
                    field_info["has_keyword_field"] = (
                        "keyword" in field_config["fields"]
                    )

                if "format" in field_config:
                    field_info["format"] = field_config["format"]

                simplified[field_name] = field_info

        return simplified

    def execute_search_query(
        self, index_name: str, query: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute a search query against Elasticsearch.

        Args:
            index_name: Name of the index to search
            query: Elasticsearch query DSL

        Returns:
            Search results
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

            # Format the response for better readability
            formatted_response = {
                "total_hits": response["hits"]["total"]["value"],
                "max_score": response["hits"]["max_score"],
                "documents": [],
                "took_ms": response["took"],
            }

            # Add document sources
            for hit in response["hits"]["hits"]:
                doc = {"score": hit["_score"], "source": hit["_source"]}
                if "_id" in hit:
                    doc["id"] = hit["_id"]
                formatted_response["documents"].append(doc)

            # Add aggregations if present
            if "aggregations" in response:
                formatted_response["aggregations"] = response["aggregations"]

            return formatted_response

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


class ElasticsearchAgent:
    """Agent specialized in handling Elasticsearch operations."""

    def __init__(self):
        """Initialize the Elasticsearch agent with tools."""
        self.es_tools = ElasticsearchTools()
        self.agent = self._create_agent()

    def _create_agent(self) -> LlmAgent:
        """
        Create the LLM agent with Elasticsearch tools.

        Returns:
            Configured LlmAgent instance
        """
        # Create tools
        list_indices_tool = Tool(
            name="list_indices",
            description="List all available Elasticsearch indices with document counts and sizes",
            function=self.es_tools.list_indices,
        )

        get_mapping_tool = Tool(
            name="get_index_mapping",
            description="Get the schema/mapping of a specific Elasticsearch index",
            function=self.es_tools.get_index_mapping,
        )

        search_tool = Tool(
            name="execute_search_query",
            description="Execute a read-only search query against an Elasticsearch index",
            function=self.es_tools.execute_search_query,
        )

        # Load instructions
        instructions = self._get_agent_instructions()

        # Create the agent
        agent = LlmAgent(
            name="ElasticsearchAgent",
            model=LiteLlm("openai/gpt-4o-mini"),
            description="Specialized agent for Elasticsearch data queries and operations",
            instruction=instructions,
            tools=[list_indices_tool, get_mapping_tool, search_tool],
        )

        return agent

    def _get_agent_instructions(self) -> str:
        """
        Get the agent instructions for Elasticsearch operations.

        Returns:
            Instruction string for the agent
        """
        return """
You are an expert Elasticsearch agent specialized in data retrieval and analysis.

Your capabilities:
1. **Index Discovery**: Use list_indices to find available indices
2. **Schema Analysis**: Use get_index_mapping to understand data structure  
3. **Data Querying**: Use execute_search_query to retrieve data

**Workflow for data queries:**
1. First, discover available indices using list_indices
2. Get the schema of relevant indices using get_index_mapping
3. Construct appropriate Elasticsearch queries using the Query DSL
4. Execute queries using execute_search_query

**Security Rules:**
- Only execute read-only operations (search, aggregations, etc.)
- Never perform write operations (create, update, delete, index, bulk)
- Validate all queries for safety before execution

**Query Construction Guidelines:**
- Use proper Elasticsearch Query DSL syntax
- Include appropriate size limits (default to 10 unless specified)
- Use aggregations for analytics and counting operations
- Apply filters and date ranges when relevant
- Use proper field names based on the index mapping

**Response Format:**
- Provide clear, human-readable summaries of results
- Include relevant metrics (total hits, execution time)
- Explain any limitations or assumptions made
- Suggest follow-up queries when appropriate

Always start by exploring available indices and their schemas before constructing queries.
Be helpful, accurate, and prioritize data security.
"""


def create_elasticsearch_agent() -> ElasticsearchAgent:
    """
    Factory function to create an Elasticsearch agent.

    Returns:
        Configured ElasticsearchAgent instance
    """
    return ElasticsearchAgent()


# Example usage and testing
if __name__ == "__main__":
    # Set up basic logging for testing
    logging.basicConfig(level=logging.INFO)

    # Create agent
    es_agent = create_elasticsearch_agent()

    # Test the tools directly
    print("Testing Elasticsearch Tools:")
    print("1. Listing indices...")
    indices = es_agent.es_tools.list_indices()
    print(json.dumps(indices, indent=2))

    if indices.get("indices"):
        first_index = indices["indices"][0]["name"]
        print(f"\n2. Getting mapping for '{first_index}'...")
        mapping = es_agent.es_tools.get_index_mapping(first_index)
        print(json.dumps(mapping, indent=2))
