#!/usr/bin/env python3
"""
Streamlit runner for the LLM ElasticSearch Agent.
This file is used to properly run Streamlit with the unified agent.
"""

import sys
import os
import asyncio
import uuid
import logging
from datetime import datetime
from pathlib import Path

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

import streamlit as st
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Phoenix imports for observability (optional)
try:
    from opentelemetry import trace as trace_api
    from opentelemetry.sdk import trace as trace_sdk
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    PHOENIX_AVAILABLE = True
except ImportError:
    PHOENIX_AVAILABLE = False

from llm_es_agent.orchestrator import create_orchestrator


class StreamlitAgentApp:
    """Streamlit-specific agent application."""
    
    def __init__(self):
        self.orchestrator = None
        self.runner = None
        self.session_service = None
        self.tracer = None
        self.logger = self._setup_logging()
    
    def _setup_logging(self):
        """Set up basic logging."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        return logging.getLogger(__name__)
    
    def _setup_phoenix_tracing(self, phoenix_endpoint: str = "http://localhost:6006") -> bool:
        """Set up Phoenix tracing for observability."""
        if not PHOENIX_AVAILABLE:
            self.logger.warning("Phoenix observability not available")
            return False

        try:
            resource = Resource.create({"service.name": "llm-es-agent-streamlit"})
            tracer_provider = trace_sdk.TracerProvider(resource=resource)
            trace_api.set_tracer_provider(tracer_provider)

            otlp_exporter = OTLPSpanExporter(
                endpoint=f"{phoenix_endpoint}/v1/traces", 
                headers={}
            )
            tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
            self.tracer = trace_api.get_tracer(__name__)
            
            self.logger.info(f"✅ Phoenix tracing initialized")
            return True

        except Exception as e:
            self.logger.warning(f"Could not initialize Phoenix tracing: {str(e)}")
            return False
    
    def _initialize_agent(self):
        """Initialize the orchestrator agent."""
        try:
            self.logger.info("Initializing Orchestrator Agent")
            self.orchestrator = create_orchestrator()

            # Setup ADK Runner components
            from google.adk.runners import Runner
            from google.adk.sessions import InMemorySessionService

            self.session_service = InMemorySessionService()
            self.runner = Runner(
                agent=self.orchestrator.agent,
                app_name="llm_es_agent_streamlit",
                session_service=self.session_service,
            )

            self.logger.info("Agent initialization completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize agent: {str(e)}", exc_info=True)
            st.error(f"Failed to initialize agent: {str(e)}")
            return False
    
    async def process_query(self, query: str, user_id: str):
        """Process user query through the orchestrator agent."""
        try:
            session_id = f"session_{uuid.uuid4().hex[:8]}"
            
            # Create session with initial state containing the user query
            session = await self.session_service.create_session(
                app_name="llm_es_agent_streamlit",
                user_id=user_id,
                session_id=session_id,
                state={"original_user_query": query}  # Pass initial state here
            )
            
            # Note: No need to call set_session_state - we pass the initial state above
            # The correct way is to set state during session creation or through events
            
            from google.genai import types
            content = types.Content(role="user", parts=[types.Part(text=query)])
            
            response_text = "Agent did not produce a final response."
            event_count = 0
            
            # Process agent events
            async for event in self.runner.run_async(
                user_id=user_id, 
                session_id=session_id, 
                new_message=content
            ):
                event_count += 1
                
                if event.is_final_response():
                    if event.content and event.content.parts:
                        response_text = event.content.parts[0].text
                    break
            
            return {
                "success": True,
                "response": response_text,
                "session_id": session_id,
                "event_count": event_count,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error processing query: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }


# Initialize the app instance
@st.cache_resource
def get_app_instance():
    """Get or create the app instance."""
    app = StreamlitAgentApp()
    
    # Setup tracing
    phoenix_endpoint = os.getenv("PHOENIX_ENDPOINT", "http://localhost:6006")
    app._setup_phoenix_tracing(phoenix_endpoint)
    
    # Initialize agent
    if not app._initialize_agent():
        st.error("Failed to initialize the agent. Please check the logs.")
        st.stop()
    
    return app

# Main Streamlit App
def main():
    """Main Streamlit application."""
    
    # Configure Streamlit page
    st.set_page_config(
        page_title="LLM ElasticSearch Agent",
        page_icon="🔍",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS
    st.markdown("""
    <style>
        .main-header {
            font-size: 2.5rem;
            color: #1f77b4;
            text-align: center;
            margin-bottom: 2rem;
        }
        
        .user-message {
            background-color: #e3f2fd;
            padding: 0.8rem;
            border-radius: 10px;
            margin: 0.5rem 0;
            border-left: 4px solid #2196f3;
        }
        
        .agent-message {
            background-color: #f1f8e9;
            padding: 0.8rem;
            border-radius: 10px;
            margin: 0.5rem 0;
            border-left: 4px solid #4caf50;
        }
        
        .error-message {
            background-color: #ffebee;
            padding: 0.8rem;
            border-radius: 10px;
            margin: 0.5rem 0;
            border-left: 4px solid #f44336;
        }
        
        .info-box {
            background-color: #fff3e0;
            padding: 1rem;
            border-radius: 8px;
            border-left: 4px solid #ff9800;
            margin: 1rem 0;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Get app instance
    app = get_app_instance()
    
    # Initialize session state
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    
    if 'user_id' not in st.session_state:
        st.session_state.user_id = f"streamlit_user_{uuid.uuid4().hex[:8]}"
    
    # Sidebar
    render_sidebar()
    
    # Main interface
    render_main_interface(app)


def render_sidebar():
    """Render the Streamlit sidebar."""
    st.sidebar.markdown("## 🔧 Configuration")
    
    # User ID
    st.session_state.user_id = st.sidebar.text_input(
        "User ID", 
        value=st.session_state.user_id,
        help="Unique identifier for this session"
    )
    
    # Service links
    st.sidebar.markdown("---")
    st.sidebar.markdown("## 🔗 Service Links")
    
    phoenix_endpoint = os.getenv("PHOENIX_ENDPOINT", "http://localhost:6006")
    if PHOENIX_AVAILABLE:
        st.sidebar.markdown(f"🔍 [Phoenix Dashboard]({phoenix_endpoint})")
    
    es_host = os.getenv("ES_HOST", "http://localhost:9200")
    kibana_url = es_host.replace(":9200", ":5601")
    
    st.sidebar.markdown(f"📊 [Kibana Dashboard]({kibana_url})")
    st.sidebar.markdown(f"🔌 [ElasticSearch API]({es_host})")
    
    # Example queries
    st.sidebar.markdown("---")
    st.sidebar.markdown("## 💡 Example Queries")
    
    example_queries = [
        "How many users are in the system?",
        "Show me recent error logs",
        "What are the top 10 most active users?",
        "Find all records from last week",
        "What is ElasticSearch?",
        "How does this agent work?"
    ]
    
    for i, query in enumerate(example_queries):
        if st.sidebar.button(f"📝 {query}", key=f"example_{i}"):
            st.session_state.example_query = query
    
    # Clear chat history
    if st.sidebar.button("🗑️ Clear Chat History", type="secondary"):
        st.session_state.chat_history = []
        st.rerun()


def render_main_interface(app):
    """Render the main Streamlit interface."""
    st.markdown('<h1 class="main-header">🔍 LLM ElasticSearch Agent</h1>', unsafe_allow_html=True)
    
    # System status
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Chat Sessions", len(st.session_state.chat_history))
    
    with col2:
        agent_status = "✅ Ready" if app.orchestrator else "❌ Not Ready"
        st.metric("Agent Status", agent_status)
    
    with col3:
        tracing_status = "✅ Enabled" if app.tracer else "❌ Disabled"
        st.metric("Tracing", tracing_status)
    
    # Info box
    st.markdown("""
    <div class="info-box">
        <strong>🚀 Welcome to the LLM ElasticSearch Agent!</strong><br>
        • Ask questions about your ElasticSearch data in natural language<br>
        • Get general information about ElasticSearch and search concepts<br>
        • All operations are read-only for security<br>
        • Use the sidebar for example queries and configuration
    </div>
    """, unsafe_allow_html=True)
    
    # Chat history
    for i, chat_item in enumerate(st.session_state.chat_history):
        # User message
        st.markdown(f"""
        <div class="user-message">
            <strong>👤 You ({chat_item['timestamp'][:19]}):</strong><br>
            {chat_item['query']}
        </div>
        """, unsafe_allow_html=True)
        
        # Agent response
        if chat_item['success']:
            st.markdown(f"""
            <div class="agent-message">
                <strong>🤖 Agent:</strong><br>
                {chat_item['response']}
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="error-message">
                <strong>❌ Error:</strong><br>
                {chat_item['error']}
            </div>
            """, unsafe_allow_html=True)
    
    # Query input
    st.markdown("---")
    
    # Handle example query selection
    initial_query = ""
    if 'example_query' in st.session_state:
        initial_query = st.session_state.example_query
        del st.session_state.example_query
    
    # Input form
    with st.form(key="query_form", clear_on_submit=True):
        user_query = st.text_area(
            "Enter your query:",
            value=initial_query,
            height=100,
            placeholder="Ask me about your ElasticSearch data or general questions...",
            help="Enter your question and press Ctrl+Enter or click Submit"
        )
        
        submit_button = st.form_submit_button("🚀 Submit", type="primary")
    
    # Process query
    if submit_button and user_query.strip():
        with st.spinner("🤔 Processing your query..."):
            # Run the async query processing
            try:
                # Create new event loop for this request
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                result = loop.run_until_complete(
                    app.process_query(user_query, st.session_state.user_id)
                )
                
                # Add to chat history
                chat_entry = {
                    'query': user_query,
                    'timestamp': datetime.now().isoformat(),
                    **result
                }
                
                st.session_state.chat_history.append(chat_entry)
                st.rerun()
                
            except Exception as e:
                st.error(f"Error processing query: {str(e)}")
                
            finally:
                loop.close()


if __name__ == "__main__":
    main()