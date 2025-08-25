import logging
from typing import Dict, Any, List
from llm_es_agent.tools.connection import ElasticsearchConnection

logger = logging.getLogger(__name__)


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


class UserInteractionTools:
    """Tools for user interaction when automatic selection is ambiguous."""

    def __init__(self):
        """Initialize with shared Elasticsearch connection."""
        self.es = ElasticsearchConnection().get_client()

    def prompt_user_for_index_selection(
        self, candidate_indices: List[str], user_query: str
    ) -> Dict[str, Any]:
        """
        Prompt the user to manually select an index when automatic selection is ambiguous.

        Args:
            candidate_indices: List of candidate index names that could be relevant
            user_query: Original user query for context

        Returns:
            Dictionary containing the selected index name or error message
        """
        try:
            print(
                f"\n🤔 Multiple indices could be relevant for your query: '{user_query}'"
            )
            print("\nCandidate indices:")

            for i, index_name in enumerate(candidate_indices, 1):
                print(f"  {i}. {index_name}")

            print(
                f"  {len(candidate_indices) + 1}. Show me all available indices first"
            )

            while True:
                try:
                    choice = input(
                        f"\nPlease select an index (1-{len(candidate_indices) + 1}): "
                    ).strip()

                    if choice.lower() in ["quit", "exit", "cancel"]:
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
                            print(
                                f"  • {idx['name']} ({idx['document_count']} docs, {idx['store_size']})"
                            )

                        continue

                    if 1 <= choice_num <= len(candidate_indices):
                        selected_index = candidate_indices[choice_num - 1]
                        print(f"✅ Selected index: {selected_index}")
                        return {"selected_index": selected_index}
                    else:
                        print(
                            f"Please enter a number between 1 and {len(candidate_indices) + 1}"
                        )

                except ValueError:
                    print("Please enter a valid number")
                except (EOFError, KeyboardInterrupt):
                    return {"error": "User cancelled index selection"}

        except Exception as e:
            logger.error(f"Error in user index selection: {str(e)}")
            return {"error": f"Failed to get user selection: {str(e)}"}
