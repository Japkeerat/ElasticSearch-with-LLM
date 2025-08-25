import json
import logging
from typing import Dict, Any, List

from llm_es_agent.tools.execution_tools import QueryExecutionTools

logger = logging.getLogger(__name__)


class QueryGenerationTools:
    """Tools for generating and validating Elasticsearch queries."""

    def __init__(self):
        """Initialize query generation tools."""
        self.execution_tools = QueryExecutionTools()

    def validate_query_syntax(self, query_dsl: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate that the generated query has correct Elasticsearch DSL syntax.

        Args:
            query_dsl: The Elasticsearch query DSL to validate

        Returns:
            Dictionary containing validation results
        """
        try:
            # Basic syntax validation
            if not isinstance(query_dsl, dict):
                return {"valid": False, "error": "Query must be a JSON object"}

            # Check for required structure
            valid_root_keys = [
                "query",
                "aggs",
                "aggregations",
                "sort",
                "size",
                "from",
                "_source",
                "highlight",
            ]
            if not any(key in query_dsl for key in valid_root_keys):
                return {
                    "valid": False,
                    "error": "Query must contain at least one valid root key (query, aggs, etc.)",
                }

            # Validate JSON serialization
            try:
                json.dumps(query_dsl)
            except (TypeError, ValueError) as e:
                return {
                    "valid": False,
                    "error": f"Query is not JSON serializable: {str(e)}",
                }

            # Check for read-only operations
            if not self.execution_tools._is_read_only_query(query_dsl):
                return {
                    "valid": False,
                    "error": "Query contains unsafe write operations",
                }

            return {"valid": True, "message": "Query syntax is valid"}

        except Exception as e:
            logger.error(f"Error validating query syntax: {str(e)}")
            return {"valid": False, "error": f"Validation error: {str(e)}"}

    def validate_fields_against_schema(
        self, query_dsl: Dict[str, Any], index_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate that all fields referenced in the query exist in the index schema.

        Args:
            query_dsl: The Elasticsearch query DSL
            index_schema: The index schema/mapping

        Returns:
            Dictionary containing field validation results
        """
        try:
            # Extract all field references from the query
            referenced_fields = self._extract_field_references(query_dsl)

            # Get available fields from schema
            available_fields = self._get_available_fields(index_schema)

            # Check for missing fields
            missing_fields = []
            for field in referenced_fields:
                if field not in available_fields:
                    missing_fields.append(field)

            if missing_fields:
                return {
                    "valid": False,
                    "missing_fields": missing_fields,
                    "available_fields": list(available_fields.keys()),
                    "error": f"Fields not found in schema: {', '.join(missing_fields)}",
                }

            return {
                "valid": True,
                "referenced_fields": referenced_fields,
                "message": "All referenced fields exist in the schema",
            }

        except Exception as e:
            logger.error(f"Error validating fields against schema: {str(e)}")
            return {"valid": False, "error": f"Field validation error: {str(e)}"}

    def _extract_field_references(self, obj: Any, fields: set = None) -> List[str]:
        """
        Recursively extract field references from a query DSL object.

        Args:
            obj: Query DSL object or part of it
            fields: Set to collect field names (used for recursion)

        Returns:
            List of field names referenced in the query
        """
        if fields is None:
            fields = set()

        if isinstance(obj, dict):
            for key, value in obj.items():
                # Common field-referencing keys in Elasticsearch queries
                if key in ["field", "fields"]:
                    if isinstance(value, str):
                        fields.add(value)
                    elif isinstance(value, list):
                        fields.update(value)
                elif key in [
                    "match",
                    "term",
                    "terms",
                    "range",
                    "exists",
                    "wildcard",
                    "prefix",
                    "regexp",
                ]:
                    if isinstance(value, dict):
                        fields.update(value.keys())
                elif (
                    key == "multi_match"
                    and isinstance(value, dict)
                    and "fields" in value
                ):
                    if isinstance(value["fields"], list):
                        fields.update(value["fields"])
                else:
                    self._extract_field_references(value, fields)
        elif isinstance(obj, list):
            for item in obj:
                self._extract_field_references(item, fields)

        return list(fields)

    def _get_available_fields(self, schema: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract available field names and types from index schema.

        Args:
            schema: Index schema/mapping

        Returns:
            Dictionary mapping field names to their types
        """
        available_fields = {}

        def extract_fields(properties: Dict[str, Any], prefix: str = ""):
            for field_name, field_config in properties.items():
                full_field_name = f"{prefix}.{field_name}" if prefix else field_name

                if isinstance(field_config, dict):
                    field_type = field_config.get("type", "unknown")
                    available_fields[full_field_name] = field_type

                    # Handle nested objects
                    if "properties" in field_config:
                        extract_fields(field_config["properties"], full_field_name)

        # Handle different schema formats
        if "index_schema" in schema and isinstance(schema["index_schema"], dict):
            extract_fields(schema["index_schema"])
        elif "schema" in schema and isinstance(schema["schema"], dict):
            extract_fields(schema["schema"])
        elif isinstance(schema, dict):
            extract_fields(schema)

        return available_fields

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
