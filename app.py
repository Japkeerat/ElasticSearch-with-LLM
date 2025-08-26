#!/usr/bin/env python3
"""
Unified LLM ElasticSearch Agent Application
Supports both Streamlit web interface and terminal interface via command-line flags.
"""

import sys
import os
import argparse
import asyncio
import uuid
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Phoenix imports for observability (optional)
try:
    import phoenix as px
    from opentelemetry import trace as trace_api
    from opentelemetry.sdk import trace as trace_sdk
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    PHOENIX_AVAILABLE = True
except ImportError:
    PHOENIX_AVAILABLE = False

from llm_es_agent.orchestrator import create_orchestrator


class UnifiedAgentApp:
    """Unified application class for both interfaces."""
    
    def __init__(self):
        self.orchestrator = None
        self.runner = None
        self.session_service = None
        self.tracer = None
        self.logger = self._setup_logging()
    
    def _setup_logging(self, log_level: str = "INFO") -> logging.Logger:
        """Set up logging configuration."""
        root_folder = Path(__file__).parent
        log_folder = root_folder / "logs"
        log_folder.mkdir(exist_ok=True)
        log_file = log_folder / "unified_agent.log"

        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"

        logger = logging.getLogger()
        logger.handlers.clear()

        level = getattr(logging, log_level.upper(), logging.INFO)
        logger.setLevel(level)

        # File handler
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(log_format, date_format)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter("%(levelname)s: %(message)s")
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        return logger
    
    def _setup_phoenix_tracing(self, phoenix_endpoint: str = "http://localhost:6006") -> bool:
        """Set up Phoenix tracing for observability."""
        if not PHOENIX_AVAILABLE:
            self.logger.warning("Phoenix observability not available (missing dependencies)")
            return False

        try:
            resource = Resource.create({"service.name": "llm-es-agent-unified"})
            tracer_provider = trace_sdk.TracerProvider(resource=resource)
            trace_api.set_tracer_provider(tracer_provider)

            otlp_exporter = OTLPSpanExporter(
                endpoint=f"{phoenix_endpoint}/v1/traces", 
                headers={}
            )
            tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
            self.tracer = trace_api.get_tracer(__name__)
            
            self.logger.info(f"✅ Phoenix tracing initialized - Dashboard: {phoenix_endpoint}")
            return True

        except Exception as e:
            self.logger.warning(f"Could not initialize Phoenix tracing: {str(e)}")
            return False
    
    def _initialize_agent(self):
        """Initialize the orchestrator agent and ADK components."""
        try:
            self.logger.info("Initializing Orchestrator Agent")
            self.orchestrator = create_orchestrator()

            # Setup ADK Runner components
            from google.adk.runners import Runner
            from google.adk.sessions import InMemorySessionService

            self.session_service = InMemorySessionService()
            self.runner = Runner(
                agent=self.orchestrator.agent,
                app_name="llm_es_agent_unified",
                session_service=self.session_service,
            )

            self.logger.info("Agent initialization completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize agent: {str(e)}", exc_info=True)
            return False
    
    async def process_query(self, query: str, user_id: str) -> Dict[str, Any]:
        """Process user query through the orchestrator agent."""
        try:
            session_id = f"session_{uuid.uuid4().hex[:8]}"
            session = await self.session_service.create_session(
                app_name="llm_es_agent_unified",
                user_id=user_id,
                session_id=session_id
            )
            
            await self.session_service.set_session_state(
                app_name="llm_es_agent_unified",
                user_id=user_id,
                session_id=session_id,
                key="original_user_query",
                value=query,
            )
            
            from google.genai import types
            content = types.Content(role="user", parts=[types.Part(text=query)])
            
            response_text = "Agent did not produce a final response."
            event_count = 0
            processing_events = []
            
            # Process agent events
            async for event in self.runner.run_async(
                user_id=user_id, 
                session_id=session_id, 
                new_message=content
            ):
                event_count += 1
                processing_events.append({
                    "event_number": event_count,
                    "author": event.author,
                    "timestamp": datetime.now().isoformat()
                })
                
                if event.is_final_response():
                    if event.content and event.content.parts:
                        response_text = event.content.parts[0].text
                    break
            
            return {
                "success": True,
                "response": response_text,
                "session_id": session_id,
                "event_count": event_count,
                "processing_events": processing_events,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def run_terminal_interface(self, enable_tracing: bool = True):
        """Run the terminal interface."""
        if enable_tracing:
            self._setup_phoenix_tracing()
        
        if not self._initialize_agent():
            sys.exit(1)
        
        self._print_terminal_welcome()
        
        # Run the terminal loop
        asyncio.run(self._terminal_loop())
    
    def run_streamlit_interface(self, enable_tracing: bool = True, port: int = 8501):
        """Run the Streamlit interface using subprocess to launch streamlit properly."""
        import subprocess
        
        self.logger.info(f"🌐 Starting Streamlit web interface on port {port}")
        
        # Create a streamlit app file that imports this module
        streamlit_runner_path = Path(__file__).parent / "streamlit_runner.py"
        
        streamlit_runner_content = f'''
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from app import UnifiedAgentApp
import streamlit as st

# Initialize the app
if "app_instance" not in st.session_state:
    st.session_state.app_instance = UnifiedAgentApp()
    if {enable_tracing}:
        st.session_state.app_instance._setup_phoenix_tracing()
    if not st.session_state.app_instance._initialize_agent():
        st.error("Failed to initialize agent")
        st.stop()

app = st.session_state.app_instance
app._run_streamlit_app()
'''
        
        # Write the runner file
        with open(streamlit_runner_path, 'w') as f:
            f.write(streamlit_runner_content)
        
        # Launch Streamlit using subprocess
        try:
            cmd = [
                sys.executable, "-m", "streamlit", "run", 
                str(streamlit_runner_path),
                "--server.port", str(port),
                "--server.address", "0.0.0.0",
                "--server.headless", "true",
                "--browser.gatherUsageStats", "false",
                "--server.enableCORS", "false"
            ]
            
            self.logger.info(f"Executing: {' '.join(cmd)}")
            
            # Use exec to replace the current process (important for Docker)
            os.execvp(sys.executable, cmd)
            
        except Exception as e:
            self.logger.error(f"Failed to start Streamlit: {e}")
            sys.exit(1)
    
    def _print_terminal_welcome(self):
        """Print terminal welcome message."""
        print("\n" + "=" * 60)
        print("  LLM ElasticSearch Agent - Interactive Terminal")
        print("=" * 60)
        print("Welcome to the LLM ES Agent!")
        print("This agent can help you with ElasticSearch queries and general questions.")
        print("\nCommands:")
        print("  • Type your query and press Enter")
        print("  • Type 'quit', 'exit', or 'q' to exit")
        print("  • Type 'help' for this message")

        if PHOENIX_AVAILABLE:
            print("\n🔍 Phoenix Observability:")
            print("  • Dashboard: http://localhost:6006")
            print("  • All interactions are being traced")
        else:
            print("\n⚠️  Phoenix observability not available")

        print("=" * 60 + "\n")
    
    async def _terminal_loop(self):
        """Main terminal interaction loop."""
        USER_ID = "terminal_user_001"
        query_count = 0
        
        while True:
            try:
                user_input = input("You: ").strip()
                
                if not user_input:
                    continue
                
                if user_input.lower() in ["quit", "exit", "q"]:
                    print("Goodbye!")
                    break
                elif user_input.lower() in ["help", "h"]:
                    self._print_help()
                    continue
                
                query_count += 1
                self.logger.info(f"Processing query {query_count}: {user_input}")
                
                # Process query
                result = await self.process_query(user_input, USER_ID)
                
                if result["success"]:
                    print(f"\nAgent: {result['response']}")
                else:
                    print(f"\nError: {result['error']}")
                
                print("-" * 50)
                
            except (EOFError, KeyboardInterrupt):
                print("\nGoodbye!")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error: {str(e)}", exc_info=True)
                print(f"An unexpected error occurred: {str(e)}")
    
    def _print_help(self):
        """Print help message."""
        print("\n" + "-" * 50)
        print("HELP - LLM ES Agent Usage Examples")
        print("-" * 50)
        print("Data queries (routed to ElasticSearch):")
        print("  • How many users are in the system?")
        print("  • Show me recent error logs")
        print("  • Find all records from last week")
        print("  • What are the top 10 most active users?")
        print("")
        print("General queries (handled directly):")
        print("  • What is ElasticSearch?")
        print("  • How does this agent work?")
        print("  • Explain full-text search")
        print("-" * 50 + "\n")
    
    def _run_streamlit_app(self):
        """Run the Streamlit interface."""
        import streamlit as st
        
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
        
        # Initialize session state
        if 'chat_history' not in st.session_state:
            st.session_state.chat_history = []
        
        if 'user_id' not in st.session_state:
            st.session_state.user_id = f"streamlit_user_{uuid.uuid4().hex[:8]}"
        
        # Sidebar
        self._render_sidebar()
        
        # Main interface
        self._render_main_interface()
    
    def _render_sidebar(self):
        """Render the Streamlit sidebar."""
        import streamlit as st
        
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
        if PHOENIX_AVAILABLE:
            st.sidebar.markdown("🔍 [Phoenix Dashboard](http://0.0.0.0:6006)")
        st.sidebar.markdown("📊 [Kibana Dashboard](http://0.0.0.0:5601)")
        st.sidebar.markdown("🔌 [ElasticSearch API](http://0.0.0.0:9200)")
        
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
    
    def _render_main_interface(self):
        """Render the main Streamlit interface."""
        import streamlit as st
        
        st.markdown('<h1 class="main-header">🔍 LLM ElasticSearch Agent</h1>', unsafe_allow_html=True)
        
        # System status
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Chat Sessions", len(st.session_state.chat_history))
        
        with col2:
            agent_status = "✅ Ready" if self.orchestrator else "❌ Not Ready"
            st.metric("Agent Status", agent_status)
        
        with col3:
            tracing_status = "✅ Enabled" if self.tracer else "❌ Disabled"
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
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                try:
                    result = loop.run_until_complete(
                        self.process_query(user_query, st.session_state.user_id)
                    )
                    
                    # Add to chat history
                    chat_entry = {
                        'query': user_query,
                        'timestamp': datetime.now().isoformat(),
                        **result
                    }
                    
                    st.session_state.chat_history.append(chat_entry)
                    st.rerun()
                    
                finally:
                    loop.close()


def main():
    """Main function to parse arguments and run the appropriate interface."""
    parser = argparse.ArgumentParser(
        description="LLM ElasticSearch Agent - Unified Application",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python app.py                    # Run Streamlit web interface (default)
  python app.py --interface web    # Run Streamlit web interface
  python app.py --interface terminal  # Run terminal interface
  python app.py --no-tracing      # Disable Phoenix tracing
  
Web interface will be available at: http://localhost:8501
        """
    )
    
    parser.add_argument(
        "--interface", "-i",
        choices=["web", "terminal", "streamlit", "cli"],
        default="web",
        help="Interface to run (default: web)"
    )
    
    parser.add_argument(
        "--no-tracing",
        action="store_true",
        help="Disable Phoenix tracing"
    )
    
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=8501,
        help="Port for Streamlit interface (default: 8501)"
    )
    
    args = parser.parse_args()
    
    # Create unified app instance
    app = UnifiedAgentApp()
    
    # Determine tracing setting
    enable_tracing = not args.no_tracing
    
    # Run appropriate interface
    interface = args.interface.lower()
    
    if interface in ["web", "streamlit"]:
        print(f"🌐 Starting Streamlit web interface on port {args.port}")
        print(f"🔍 Tracing: {'Enabled' if enable_tracing else 'Disabled'}")
        print(f"📱 Visit: http://localhost:{args.port}")
        
        # Run Streamlit using proper method
        app.run_streamlit_interface(enable_tracing=enable_tracing, port=args.port)
        
    elif interface in ["terminal", "cli"]:
        print("💻 Starting terminal interface")
        print(f"🔍 Tracing: {'Enabled' if enable_tracing else 'Disabled'}")
        app.run_terminal_interface(enable_tracing=enable_tracing)
    
    else:
        parser.error(f"Unknown interface: {interface}")


if __name__ == "__main__":
    main()