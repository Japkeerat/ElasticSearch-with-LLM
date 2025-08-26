import logging
from pathlib import Path
from typing import Optional, Dict, Any, List

from pydantic import BaseModel, Field
from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm
from google.adk.tools import FunctionTool

from llm_es_agent.tools.execution_tools import QueryExecutionTools
from llm_es_agent.tools.session_tools import save_execution_results_data, get_session_data, get_user_query

logger = logging.getLogger(__name__)


class ExecutionResults(BaseModel):
    """Elasticsearch query execution results."""

    total_hits: int = Field(description="Total number of documents matching the query")
    execution_time_ms: int = Field(description="Query execution time in milliseconds")
    documents: List[Dict[str, Any]] = Field(
        description="Retrieved documents with scores and sources"
    )
    aggregations: Dict[str, Any] = Field(description="Aggregation results if present")
    query_metadata: Dict[str, Any] = Field(
        description="Metadata about the executed query"
    )


class QueryExecutionOutput(BaseModel):
    """Structured output for the Query Execution Agent."""

    execution_results: Optional[ExecutionResults] = Field(
        description="Raw query execution results from Elasticsearch, or null if execution failed"
    )
    success: bool = Field(description="Whether the query execution was successful")
    error_message: Optional[str] = Field(
        description="Error message if execution failed"
    )
    natural_language_response: str = Field(
        description="Natural language response based on analyzing the actual results"
    )


class QueryExecutionAgent:
    """Agent specialized in executing Elasticsearch queries and presenting results."""

    def __init__(self):
        """Initialize the Query Execution agent with tools."""
        self.execution_tools = QueryExecutionTools()
        self.agent = self._create_agent()

    def _create_agent(self) -> LlmAgent:
        """
        Create the LLM agent for query execution.

        Returns:
            Configured LlmAgent instance
        """
        # Create tools - FunctionTool automatically extracts name and description from function
        execute_query_tool = FunctionTool(self.execution_tools.execute_query)
        
        # Add session state management tools
        save_execution_data_tool = FunctionTool(save_execution_results_data)
        get_session_data_tool = FunctionTool(get_session_data)
        get_user_query_tool = FunctionTool(get_user_query)

        # Load instructions from prompt file
        instructions = self._get_agent_instructions()

        # FIXED: Remove output_schema to allow natural language responses
        agent = LlmAgent(
            name="QueryExecutionAgent",
            model=LiteLlm("openai/gpt-4o-mini"),
            description="Specialized agent for executing Elasticsearch queries and presenting results in natural language",
            instruction=instructions,
            tools=[
                execute_query_tool,
                save_execution_data_tool,
                get_session_data_tool,
                get_user_query_tool
            ],
            # Removed output_schema - this agent provides the final natural language response
            output_key="query_execution_result",
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
            / "query_execution_agent.txt"
        )
        with open(file_path, "r") as f:
            prompt = f.read()
        return prompt


def create_query_execution_agent() -> QueryExecutionAgent:
    """
    Factory function to create a Query Execution agent.

    Returns:
        Configured QueryExecutionAgent instance
    """
    return QueryExecutionAgent()


# Example usage and testing
if __name__ == "__main__":
    # Set up basic logging for testing
    logging.basicConfig(level=logging.INFO)

    # Create agent
    execution_agent = create_query_execution_agent()
    print("Query Execution Agent created successfully")