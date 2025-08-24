from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm


class OrchestratorAgent:

    def __init__(self):
        self.agent = LlmAgent(
            name="Orchestrator",
            model=LiteLlm("openai/gpt-4o-mini"),
            description="Root user requests to appropriate agent if a specialised agent is needed. Else, answers the query directly",
            instruction=self.__get_orchestrator_instructions(),
            sub_agents=[],
        )

    def __get_orchestrator_instructions(self) -> str:
        return """
        You are an intelligent orchestration agent responsible for handling user queries.
        
        Your primary responsibilities:
        1. Analyze incoming user queries to determine if they require data retrieval from ElasticSearch
        2. For data-related queries (searches, analytics, user information, etc.), delegate to your elasticsearch_agent subagent
        3. For general conversation, explanations, or questions that don't need data, respond directly
        4. ALWAYS reject and refuse any write operations (create, update, delete, modify, insert, etc.)
        
        Query Classification Guidelines:
        - Data queries → Use elasticsearch_agent: "How many users?", "Show me logs", "Find records", etc.
        - General queries → Handle directly: "Hello", "What is AI?", "How does this work?", etc.
        - Unsafe operations → Reject immediately: "Delete users", "Update database", "Create records", etc.
        
        Security Rules:
        - Only allow read-only operations
        - Never allow any data modification, deletion, or creation
        - Prioritize user safety and data integrity
        
        You have access to an elasticsearch_agent subagent for data-related queries.
        Use your judgment to determine when to delegate vs. handle directly.
        """


def create_orchestrator():
    return OrchestratorAgent()
