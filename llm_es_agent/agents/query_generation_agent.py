import logging
from pathlib import Path
from typing import Optional, Dict, Any, List

from pydantic import BaseModel, Field
from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm
from google.adk.tools import FunctionTool

from llm_es_agent.tools.query_tools import QueryGenerationTools

logger = logging.getLogger(__name__)


class GeneratedQuery(BaseModel):
    """Generated Elasticsearch query information."""

    query_dsl: Dict[str, Any] = Field(description="Complete Elasticsearch query DSL")
    query_type: str = Field(
        description="Type of query: 'match', 'term', 'range', 'bool', 'aggregation', etc."
    )
    target_fields: List[str] = Field(description="List of fields targeted by the query")
    estimated_complexity: str = Field(
        description="Query complexity: 'simple', 'medium', 'complex'"
    )


class QueryMetadata(BaseModel):
    """Metadata about the query generation process."""

    generation_method: str = Field(
        description="Method used: 'automatic', 'field_analysis', 'user_clarification', or 'error'"
    )
    user_intent: str = Field(
        description="Interpreted user intent from the natural language query"
    )
    field_mappings: Dict[str, str] = Field(
        description="Mapping of user terms to actual index fields"
    )
    reasoning: str = Field(description="Explanation of how the query was constructed")
    confidence: str = Field(
        description="Confidence level: 'high', 'medium', 'low', or 'none'"
    )


class QueryValidation(BaseModel):
    """Validation results for the generated query."""

    syntax_valid: bool = Field(
        description="Whether the query has valid Elasticsearch DSL syntax"
    )
    fields_exist: bool = Field(
        description="Whether all referenced fields exist in the index schema"
    )
    query_safe: bool = Field(
        description="Whether the query is read-only and safe to execute"
    )
    ready_for_execution: bool = Field(
        description="Whether the query is ready to be executed"
    )


class QueryGenerationOutput(BaseModel):
    """Structured output for the Query Generation Agent."""

    generated_query: Optional[GeneratedQuery] = Field(
        description="The generated Elasticsearch query, or null if generation failed"
    )
    target_index: str = Field(description="Name of the target index for the query")
    query_metadata: QueryMetadata = Field(
        description="Metadata about the query generation process"
    )
    validation: QueryValidation = Field(
        description="Validation results for the generated query"
    )


class QueryGenerationAgent:
    """Agent specialized in generating Elasticsearch queries from natural language."""

    def __init__(self):
        """Initialize the Query Generation agent with tools."""
        self.query_tools = QueryGenerationTools()
        self.agent = self._create_agent()

    def _create_agent(self) -> LlmAgent:
        """
        Create the LLM agent for query generation.

        Returns:
            Configured LlmAgent instance
        """
        # Create tools - FunctionTool automatically extracts name and description from function
        validate_syntax_tool = FunctionTool(self.query_tools.validate_query_syntax)
        validate_fields_tool = FunctionTool(
            self.query_tools.validate_fields_against_schema
        )

        # Load instructions from prompt file
        instructions = self._get_agent_instructions()

        # Create the agent with output schema and output key for state management
        agent = LlmAgent(
            name="QueryGenerationAgent",
            model=LiteLlm("openai/gpt-4o-mini"),
            description="Specialized agent for generating Elasticsearch queries from natural language",
            instruction=instructions,
            tools=[validate_syntax_tool, validate_fields_tool],
            output_schema=QueryGenerationOutput,  # Enforces JSON output structure
            output_key="query_generation_result",  # ADK will automatically save final response to session state
        )

        return agent

    def _get_agent_instructions(self) -> str:
        """
        Load the agent instructions from the prompts directory.

        Returns:
            Instruction string for the agent
        """
        file_path = (
            Path(__file__).parent.parent.parent
            / "prompts"
            / "query_generation_agent.txt"
        )
        with open(file_path, "r") as f:
            prompt = f.read()
        return prompt


def create_query_generation_agent() -> QueryGenerationAgent:
    """
    Factory function to create a Query Generation agent.

    Returns:
        Configured QueryGenerationAgent instance
    """
    return QueryGenerationAgent()


# Example usage and testing
if __name__ == "__main__":
    # Set up basic logging for testing
    logging.basicConfig(level=logging.INFO)

    # Create agent
    query_agent = create_query_generation_agent()

    # Test the tools directly
    print("Testing Query Generation Tools:")

    # Test query validation
    test_query = {"query": {"match": {"title": "elasticsearch"}}, "size": 10}

    print("1. Testing query syntax validation...")
    validation_result = query_agent.query_tools.validate_query_syntax(test_query)
    print(f"Validation result: {validation_result}")

    # Test field validation with mock schema
    mock_schema = {
        "schema": {
            "title": {"type": "text"},
            "content": {"type": "text"},
            "timestamp": {"type": "date"},
        }
    }

    print("\n2. Testing field validation against schema...")
    field_validation = query_agent.query_tools.validate_fields_against_schema(
        test_query, mock_schema
    )
    print(f"Field validation result: {field_validation}")
