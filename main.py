import sys
import uuid
import logging
import asyncio
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Phoenix imports for observability (optional)
try:
    import phoenix as px
    from opentelemetry import trace as trace_api
    from opentelemetry.sdk import trace as trace_sdk
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    PHOENIX_AVAILABLE = True
except ImportError as e:
    PHOENIX_AVAILABLE = False

from llm_es_agent.orchestrator import create_orchestrator


# Load environment variables
load_dotenv()


def setup_phoenix_tracing(phoenix_endpoint: str = "http://localhost:6006") -> bool:
    """
    Set up Phoenix tracing for observability.

    Args:
        phoenix_endpoint: Phoenix server endpoint URL

    Returns:
        bool: True if tracing was set up successfully
    """
    if not PHOENIX_AVAILABLE:
        print("⚠️  Phoenix observability not available (missing dependencies)")
        print(
            "To enable: pip install arize-phoenix opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp"
        )
        return False

    try:
        # Configure OpenTelemetry tracer
        resource = Resource.create({"service.name": "llm-es-agent"})

        tracer_provider = trace_sdk.TracerProvider(resource=resource)
        trace_api.set_tracer_provider(tracer_provider)

        # Set up OTLP exporter for Phoenix
        otlp_exporter = OTLPSpanExporter(
            endpoint=f"{phoenix_endpoint}/v1/traces", headers={}
        )

        # Add the exporter to the tracer provider
        tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

        print(f"✅ Phoenix tracing initialized - Dashboard: {phoenix_endpoint}")
        return True

    except Exception as e:
        print(f"⚠️  Warning: Could not initialize Phoenix tracing: {str(e)}")
        print("Continuing without observability...")
        return False


def create_custom_tracer():
    """Create a custom tracer for manual instrumentation."""
    if not PHOENIX_AVAILABLE:
        return None
    return trace_api.get_tracer(__name__)


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """
    Set up logging configuration with both file and console handlers.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        Configured logger instance
    """
    # Create logs directory
    root_folder = Path(__file__).parent
    log_folder = root_folder / "logs"
    log_folder.mkdir(exist_ok=True)

    log_file = log_folder / "es_agent.log"

    # Configure logging format
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # Clear any existing handlers
    logger = logging.getLogger()
    logger.handlers.clear()

    # Set logging level
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)

    # File handler - logs everything
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(log_format, date_format)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler - logs INFO and above
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter("%(levelname)s: %(message)s")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger


def print_welcome_message():
    """Print welcome message and usage instructions."""
    print("\n" + "=" * 60)
    print("  LLM ElasticSearch Agent - Interactive Terminal")
    print("=" * 60)
    print("Welcome to the LLM ES Agent!")
    print("This agent can help you with ElasticSearch queries and general questions.")
    print("\nCommands:")
    print("  • Type your query and press Enter")
    print("  • Type 'quit', 'exit', or 'q' to exit")
    print("  • Type 'help' for this message")

    # Check if Phoenix is available
    if PHOENIX_AVAILABLE:
        print("\n🔍 Phoenix Observability:")
        print("  • Dashboard: http://localhost:6006")
        print("  • All interactions are being traced")
    else:
        print("\n⚠️  Phoenix observability not available")

    print("=" * 60 + "\n")


def print_help():
    """Print help message with usage examples."""
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
    print("")
    print("Security Note:")
    print("  • Only read operations are allowed")
    print("  • Write/Update/Delete operations are rejected")
    print("-" * 50 + "\n")


def get_user_input() -> Optional[str]:
    """
    Get user input from terminal with error handling.

    Returns:
        User input string or None if error/EOF
    """
    try:
        user_input = input("You: ").strip()
        return user_input
    except (EOFError, KeyboardInterrupt):
        return None


async def process_user_query(
    runner,
    session_service,
    query: str,
    user_id: str,
    app_name: str,
    logger: logging.Logger,
    tracer=None,
) -> None:
    """
    Process user query through the orchestrator agent using the ADK Runner.

    Args:
        runner: The ADK Runner instance
        session_service: The session service instance
        query: User query string
        user_id: User ID for the session
        app_name: Application name
        logger: Logger instance
        tracer: OpenTelemetry tracer for observability
    """
    # Create a span for the entire query processing
    span_name = "process_user_query"
    if tracer:
        with tracer.start_as_current_span(span_name) as span:
            span.set_attribute("user.id", user_id)
            span.set_attribute("user.query", query)
            span.set_attribute("app.name", app_name)
            await _process_query_internal(
                runner, session_service, query, user_id, app_name, logger, span
            )
    else:
        await _process_query_internal(
            runner, session_service, query, user_id, app_name, logger, None
        )


async def _process_query_internal(
    runner,
    session_service,
    query: str,
    user_id: str,
    app_name: str,
    logger: logging.Logger,
    span=None,
):
    """Internal function to process the query with optional span tracking."""
    try:
        logger.info(f"Processing user query: {query}")

        # Create a new session for this query
        session_id = f"session_{uuid.uuid4().hex[:8]}"
        session = await session_service.create_session(
            app_name=app_name, user_id=user_id, session_id=session_id
        )
        logger.debug(f"Created new session: {session_id}")

        # Initialize session state with the original user query (ADK pattern)
        await session_service.set_session_state(
            app_name=app_name,
            user_id=user_id,
            session_id=session_id,
            key="original_user_query",
            value=query,
        )
        logger.debug(f"Initialized session state with user query: {query}")

        if span:
            span.set_attribute("session.id", session_id)

        # Prepare the user message in ADK format
        from google.genai import types

        content = types.Content(role="user", parts=[types.Part(text=query)])

        # Send query to orchestrator agent via runner
        final_response_text = "Agent did not produce a final response."
        event_count = 0

        if span:
            span.add_event("agent_execution_started")

        async for event in runner.run_async(
            user_id=user_id, session_id=session_id, new_message=content
        ):
            event_count += 1
            logger.debug(f"Received event {event_count}: {event.author}")

            # Check for final response
            if event.is_final_response():
                if event.content and event.content.parts:
                    final_response_text = event.content.parts[0].text
                    if span:
                        span.set_attribute("response.length", len(final_response_text))
                        span.add_event("final_response_received")
                    break

        if span:
            span.set_attribute("events.total_count", event_count)
            span.set_attribute(
                "response.text", final_response_text[:500]
            )  # Truncate for span
            if PHOENIX_AVAILABLE:
                span.set_status(trace_api.StatusCode.OK)

        logger.info(f"Agent response generated successfully")

        print(f"\nAgent: {final_response_text}")
        print("-" * 50)

    except Exception as e:
        logger.error(f"Error processing query '{query}': {str(e)}", exc_info=True)

        if span and PHOENIX_AVAILABLE:
            span.record_exception(e)
            span.set_status(trace_api.StatusCode.ERROR, str(e))

        print(f"\nSorry, I encountered an error processing your query: {str(e)}")
        print("Please try again with a different query.")
        print("-" * 50)


async def run_application_logic(logger: logging.Logger, tracer):
    """Run the main application logic with optional tracing."""
    try:
        # Initialize orchestrator agent
        logger.info("Initializing Orchestrator Agent")

        if tracer:
            with tracer.start_as_current_span("initialize_orchestrator"):
                orchestrator_agent = create_orchestrator()
        else:
            orchestrator_agent = create_orchestrator()

        logger.info("Orchestrator Agent initialized successfully")

        # Setup ADK Runner components
        from google.adk.runners import Runner
        from google.adk.sessions import InMemorySessionService

        # Constants for the session
        APP_NAME = "llm_es_agent"
        USER_ID = "user_001"

        # Initialize session service
        session_service = InMemorySessionService()

        # Create the runner
        runner = Runner(
            agent=orchestrator_agent.agent,
            app_name=APP_NAME,
            session_service=session_service,
        )

        logger.info("ADK Runner initialized successfully")

        # Print welcome message
        print_welcome_message()

        # Main interaction loop
        query_count = 0
        while True:
            try:
                # Get user input
                user_input = get_user_input()

                # Handle None input (EOF, Ctrl+C)
                if user_input is None:
                    print("\nGoodbye!")
                    break

                # Handle empty input
                if not user_input:
                    continue

                # Handle special commands
                if user_input.lower() in ["quit", "exit", "q"]:
                    print("Goodbye!")
                    logger.info("User requested exit")
                    break
                elif user_input.lower() in ["help", "h"]:
                    print_help()
                    continue

                # Increment query counter for tracking
                query_count += 1

                # Process regular query with tracing
                if tracer:
                    with tracer.start_as_current_span(
                        "user_interaction"
                    ) as interaction_span:
                        interaction_span.set_attribute("query.number", query_count)
                        interaction_span.set_attribute("query.type", "user_query")
                        await process_user_query(
                            runner,
                            session_service,
                            user_input,
                            USER_ID,
                            APP_NAME,
                            logger,
                            tracer,
                        )
                else:
                    await process_user_query(
                        runner,
                        session_service,
                        user_input,
                        USER_ID,
                        APP_NAME,
                        logger,
                        None,
                    )

            except KeyboardInterrupt:
                print("\n\nReceived interrupt signal. Goodbye!")
                logger.info("Application interrupted by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {str(e)}", exc_info=True)
                print(f"An unexpected error occurred: {str(e)}")
                print("Continuing...")

    except Exception as e:
        logger.error(f"Failed to initialize application: {str(e)}", exc_info=True)
        print(f"Failed to start application: {str(e)}")
        sys.exit(1)

    finally:
        logger.info("LLM ES Agent application terminated")
        if tracer and PHOENIX_AVAILABLE:
            # Flush any remaining traces
            try:
                trace_api.get_tracer_provider().force_flush(timeout_millis=5000)
            except:
                pass


def main():
    """Main application loop."""

    async def async_main():
        # Setup Phoenix tracing first
        phoenix_enabled = setup_phoenix_tracing()
        tracer = create_custom_tracer() if phoenix_enabled else None

        # Setup logging
        logger = setup_logging()
        logger.info("Starting LLM ES Agent application")

        # Create application span if tracing is enabled
        if tracer:
            with tracer.start_as_current_span("llm_es_agent_application") as app_span:
                app_span.set_attribute("app.version", "1.0.0")
                app_span.set_attribute("app.name", "llm_es_agent")
                await run_application_logic(logger, tracer)
        else:
            await run_application_logic(logger, None)

    # Run the async main function - THIS WAS THE MISSING PIECE!
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
