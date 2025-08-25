from pathlib import Path

from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm
from llm_es_agent.pipeline_agent import create_elasticsearch_agent


class OrchestratorAgent:

    def __init__(self):
        # Create the Elasticsearch sub-agent
        es_agent = create_elasticsearch_agent()

        self.agent = LlmAgent(
            name="Orchestrator",
            model=LiteLlm("openai/gpt-4o-mini"),
            description="Root user requests to appropriate agent if a specialised agent is needed. Else, answers the query directly",
            instruction=self.__get_orchestrator_instructions(),
            sub_agents=[es_agent.agent],  # Add the ES agent as a sub-agent
        )

    def __get_orchestrator_instructions(self) -> str:
        file_path = Path(__file__).parent.parent / "prompts" / "orchestrator.txt"
        with open(file_path, "r") as f:
            prompt = f.read()
        return prompt


def create_orchestrator():
    return OrchestratorAgent()
