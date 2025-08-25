import os
import json
import logging
from typing import Dict, Any, List

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


class IndexDiscoveryTools:
    """Tools for discovering and analyzing Elasticsearch indices."""
    
    def __init__(self):
        """Initialize with shared Elasticsearch connection."""
        self.es = ElasticsearchConnection().get_client()
    
    def list_indices(self) -> Dict[str, Any]:
        """
        Get list of all available Elasticsearch indices with document counts and sizes.
        
        Returns:
            Dictionary containing indices information with names, document counts, and sizes
        """
        try:
            # Get indices with stats
            indices_response = self.es.cat.indices(format="json", h="index,docs.count,store.size")
            
            if not indices_response:
                return {"indices": [], "total_count": 0}
            
            indices_info = []
            for index in indices_response:
                indices_info.append({
                    "name": index.get("index", ""),
                    "document_count": index.get("docs.count", "0"),
                    "store_size": index.get("store.size", "0b")
                })
            
            return {
                "indices": indices_info,
                "total_count": len(indices_info)
            }
            
        except Exception as e:
            logger.error(f"Error listing indices: {str(e)}")
            return {"error": f"Failed to list indices: {str(e)}"}

    def get_index_mapping(self, index_name: str) -> Dict[str, Any]:
        """
        Get the mapping/schema for a specific Elasticsearch index to understand its structure.
        
        Args:
            index_name: Name of the Elasticsearch index
            
        Returns:
            Dictionary containing the index mapping with simplified schema structure
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
                "properties_count": len(properties)
            }
            
        except Exception as e:
            logger.error(f"Error getting mapping for index {index_name}: {str(e)}")
            return {"error": f"Failed to get mapping for index '{index_name}': {str(e)}"}

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
                    "properties": self._simplify_mapping(field_config["properties"])
                }
            elif field_type == "nested" and "properties" in field_config:
                simplified[field_name] = {
                    "type": "nested",
                    "properties": self._simplify_mapping(field_config["properties"])
                }
            else:
                # Handle simple types
                field_info = {"type": field_type}
                
                # Add additional useful information
                if "fields" in field_config:
                    field_info["has_keyword_field"] = "keyword" in field_config["fields"]
                
                if "format" in field_config:
                    field_info["format"] = field_config["format"]
                    
                simplified[field_name] = field_info
        
        return simplified


class UserInteractionTools:
    """Tools for user interaction when automatic selection is ambiguous."""
    
    def __init__(self):
        """Initialize with shared Elasticsearch connection."""
        self.es = ElasticsearchConnection().get_client()
    
    def prompt_user_for_index_selection(self, candidate_indices: List[str], user_query: str) -> Dict[str, Any]:
        """
        Prompt the user to manually select an index when automatic selection is ambiguous.
        
        Args:
            candidate_indices: List of candidate index names that could be relevant
            user_query: Original user query for context
            
        Returns:
            Dictionary containing the selected index name or error message
        """
        try:
            print(f"\n🤔 Multiple indices could be relevant for your query: '{user_query}'")
            print("\nCandidate indices:")
            
            for i, index_name in enumerate(candidate_indices, 1):
                print(f"  {i}. {index_name}")
            
            print(f"  {len(candidate_indices) + 1}. Show me all available indices first")
            
            while True:
                try:
                    choice = input(f"\nPlease select an index (1-{len(candidate_indices) + 1}): ").strip()
                    
                    if choice.lower() in ['quit', 'exit', 'cancel']:
                        return {"error": "User cancelled index selection"}
                    
                    choice_num = int(choice)
                    
                    if choice_num == len(candidate_indices) + 1:
                        # User wants to see all indices
                        discovery_tools = IndexDiscoveryTools()
                        all_indices = discovery_tools.list_indices()
                        if "error" in all_indices:
                            return all_indices
                        
                        print("\nAll available indices:")
                        for idx in all_indices["indices"]:
                            print(f"  • {idx['name']} ({idx['document_count']} docs, {idx['store_size']})")
                        
                        continue
                    
                    if 1 <= choice_num <= len(candidate_indices):
                        selected_index = candidate_indices[choice_num - 1]
                        print(f"✅ Selected index: {selected_index}")
                        return {"selected_index": selected_index}
                    else:
                        print(f"Please enter a number between 1 and {len(candidate_indices) + 1}")
                        
                except ValueError:
                    print("Please enter a valid number")
                except (EOFError, KeyboardInterrupt):
                    return {"error": "User cancelled index selection"}
                    
        except Exception as e:
            logger.error(f"Error in user index selection: {str(e)}")
            return {"error": f"Failed to get user selection: {str(e)}"}


class QueryExecutionTools:
    """Tools for executing queries against Elasticsearch."""
    
    def __init__(self):
        """Initialize with shared Elasticsearch connection."""
        self.es = ElasticsearchConnection().get_client()
    
    def execute_search_query(self, index_name: str, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a read-only search query against an Elasticsearch index.
        
        Args:
            index_name: Name of the index to search
            query: Elasticsearch query DSL dictionary
            
        Returns:
            Formatted search results with documents, aggregations, and metadata
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
                "took_ms": response["took"]
            }
            
            # Add document sources
            for hit in response["hits"]["hits"]:
                doc = {
                    "score": hit["_score"],
                    "source": hit["_source"]
                }
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
            "update", "delete", "create", "index", "bulk", 
            "_update", "_delete", "_create", "script"
        ]
        
        for operation in write_operations:
            if operation in query_str:
                logger.warning(f"Potentially unsafe operation '{operation}' detected in query")
                return False
        
        return True
