import logging
from pathlib import Path
from typing import Dict, Any, Optional

from pydantic import BaseModel, Field
from google.adk.agents import SequentialAgent, LlmAgent
from google.adk.models.lite_llm import LiteLlm

from llm_es_agent.agents.index_selection_agent import create_index_selection_agent
from llm_es_agent.agents.query_generation_agent import create_query_generation_agent
from llm_es_agent.agents.query_execution_agent import create_query_execution_agent

logger = logging.getLogger(__name__)


class PipelineResult(BaseModel):
    """Result from the complete Elasticsearch pipeline."""

    pipeline_status: str = Field(
        description="Status: 'success', 'partial_success', 'failed'"
    )
    stage_completed: str = Field(
        description="Last completed stage: 'index_selection', 'query_generation', 'query_execution'"
    )
    final_response: str = Field(
        description="Final natural language response to the user"
    )
    execution_metadata: Dict[str, Any] = Field(
        description="Metadata from all pipeline stages"
    )
    error_details: Optional[Dict[str, Any]] = Field(
        description="Error details if pipeline failed"
    )


class ElasticsearchPipelineAgent:
    """
    Agent that orchestrates the complete Elasticsearch query pipeline:
    1. Index Selection Agent - Selects the appropriate index
    2. Query Generation Agent - Generates the Elasticsearch query
    3. Query Execution Agent - Executes query and formats results
    """

    def __init__(self):
        """Initialize the pipeline agent with all sub-agents."""
        # Create specialized agents
        self.index_selection_agent = create_index_selection_agent()
        self.query_generation_agent = create_query_generation_agent()
        self.query_execution_agent = create_query_execution_agent()

        # Create the main pipeline agent
        self.agent = self._create_agent()

    def _create_agent(self) -> SequentialAgent:
        """
        Create the main pipeline agent using SequentialAgent with explicit configuration.

        Returns:
            Configured SequentialAgent instance
        """

        # Create SequentialAgent following ADK best practices
        # Each sub-agent keeps its output_key for state management
        agent = SequentialAgent(
            name="ElasticsearchPipelineAgent",
            description="Orchestrates the complete Elasticsearch query pipeline from natural language to results",
            sub_agents=[
                self.index_selection_agent.agent,
                self.query_generation_agent.agent,
                self.query_execution_agent.agent,
            ],
        )

        return agent


def create_elasticsearch_pipeline_agent() -> ElasticsearchPipelineAgent:
    """
    Factory function to create an Elasticsearch Pipeline agent.

    Returns:
        Configured ElasticsearchPipelineAgent instance
    """
    return ElasticsearchPipelineAgent()


# Use wrapper approach for reliable pipeline execution
def create_elasticsearch_agent() -> ElasticsearchPipelineAgent:
    """
    Factory function for backward compatibility.
    Uses the wrapper approach for reliable pipeline execution.

    Returns:
        Configured ElasticsearchPipelineAgentWrapper instance
    """
    return create_elasticsearch_pipeline_agent()
