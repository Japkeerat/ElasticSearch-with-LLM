import logging
from pathlib import Path
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field
from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm
from google.adk.tools import FunctionTool

from llm_es_agent.tools.index_tools import IndexDiscoveryTools, UserInteractionTools

logger = logging.getLogger(__name__)


class IndexSchema(BaseModel):
    """Schema information for the selected index."""

    index: str = Field(description="Name of the index")
    index_schema: Dict[str, Any] = Field(
        description="Simplified index_schema structure with field types"
    )
    properties_count: int = Field(
        description="Number of properties/fields in the index"
    )


class SelectionMetadata(BaseModel):
    """Metadata about how the index was selected."""

    selection_method: str = Field(
        description="Method used for selection: 'automatic', 'schema_analysis', 'user_input', or 'error'"
    )
    candidate_indices: List[str] = Field(
        description="List of candidate indices that were considered"
    )
    reasoning: str = Field(
        description="Brief explanation of why this index was selected"
    )
    confidence: str = Field(
        description="Confidence level: 'high', 'medium', 'low', or 'none'"
    )


class ValidationResult(BaseModel):
    """Validation results for the selected index."""

    index_exists: bool = Field(
        description="Whether the selected index exists in Elasticsearch"
    )
    schema_retrieved: bool = Field(
        description="Whether the schema was successfully retrieved"
    )
    ready_for_query_generation: bool = Field(
        description="Whether the selection is ready for the next pipeline stage"
    )


class IndexSelectionOutput(BaseModel):
    """Structured output for the Index Selection Agent."""

    selected_index: Optional[str] = Field(
        description="Name of the selected index, or null if selection failed"
    )
    index_schema: Optional[IndexSchema] = Field(
        description="Complete schema information for the selected index"
    )
    selection_metadata: SelectionMetadata = Field(
        description="Metadata about the selection process"
    )
    validation: ValidationResult = Field(
        description="Validation results for the selection"
    )


class IndexSelectionAgent:
    """Agent specialized in selecting the most appropriate Elasticsearch index for a query."""

    def __init__(self):
        """Initialize the Index Selection agent with tools."""
        self.discovery_tools = IndexDiscoveryTools()
        self.interaction_tools = UserInteractionTools()
        self.agent = self._create_agent()

    def _create_agent(self) -> LlmAgent:
        """
        Create the LLM agent for index selection.

        Returns:
            Configured LlmAgent instance
        """
        # Create tools - FunctionTool automatically extracts name and description from function
        list_indices_tool = FunctionTool(self.discovery_tools.list_indices)
        get_mapping_tool = FunctionTool(self.discovery_tools.get_index_mapping)
        user_selection_tool = FunctionTool(
            self.interaction_tools.prompt_user_for_index_selection
        )

        # Load instructions from prompt file
        instructions = self._get_agent_instructions()

        # FIXED: Remove output_schema to allow natural language responses
        # The agent will still save structured data to session state via tools/callbacks
        agent = LlmAgent(
            name="IndexSelectionAgent",
            model=LiteLlm("openai/gpt-4o-mini"),
            description="Specialized agent for selecting the most appropriate Elasticsearch index for a user query",
            instruction=instructions,
            tools=[list_indices_tool, get_mapping_tool, user_selection_tool],
            # Removed output_schema - let the agent respond naturally
            output_key="index_selection_result",  # Still save the response to session state
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
            / "index_selection_agent.txt"
        )
        with open(file_path, "r") as f:
            prompt = f.read()
        return prompt


def create_index_selection_agent() -> IndexSelectionAgent:
    """
    Factory function to create an Index Selection agent.

    Returns:
        Configured IndexSelectionAgent instance
    """
    return IndexSelectionAgent()


# Example usage and testing
if __name__ == "__main__":
    # Set up basic logging for testing
    logging.basicConfig(level=logging.INFO)

    # Create agent
    index_agent = create_index_selection_agent()

    # Test the tools directly
    print("Testing Index Selection Tools:")
    print("1. Listing indices...")
    indices = index_agent.discovery_tools.list_indices()
    print(f"Found {indices.get('total_count', 0)} indices")

    if indices.get("indices"):
        first_index = indices["indices"][0]["name"]
        print(f"\n2. Getting mapping for '{first_index}'...")
        mapping = index_agent.discovery_tools.get_index_mapping(first_index)
        print(
            f"Index '{first_index}' has {mapping.get('properties_count', 0)} properties"
        )
    else:
        print("No indices found to test mapping")