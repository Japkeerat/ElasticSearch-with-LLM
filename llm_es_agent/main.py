import logging
import sys
import asyncio
import uuid
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from orchestrator import create_orchestrator


# Load environment variables
load_dotenv()


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
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
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
    print("\n" + "="*60)
    print("  LLM ElasticSearch Agent - Interactive Terminal")
    print("="*60)
    print("Welcome to the LLM ES Agent!")
    print("This agent can help you with ElasticSearch queries and general questions.")
    print("\nCommands:")
    print("  • Type your query and press Enter")
    print("  • Type 'quit', 'exit', or 'q' to exit")
    print("  • Type 'help' for this message")
    print("="*60 + "\n")


def print_help():
    """Print help message with usage examples."""
    print("\n" + "-"*50)
    print("HELP - LLM ES Agent Usage Examples")
    print("-"*50)
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
    print("-"*50 + "\n")


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


async def process_user_query(runner, session_service, query: str, user_id: str, app_name: str, logger: logging.Logger) -> None:
    """
    Process user query through the orchestrator agent using the ADK Runner.
    
    Args:
        runner: The ADK Runner instance
        session_service: The session service instance
        query: User query string
        user_id: User ID for the session
        app_name: Application name
        logger: Logger instance
    """
    try:
        logger.info(f"Processing user query: {query}")
        
        # Create a new session for this query
        session_id = f"session_{uuid.uuid4().hex[:8]}"
        session = await session_service.create_session(
            app_name=app_name,
            user_id=user_id,
            session_id=session_id
        )
        logger.debug(f"Created new session: {session_id}")
        
        # Prepare the user message in ADK format
        from google.genai import types
        content = types.Content(role='user', parts=[types.Part(text=query)])
        
        # Send query to orchestrator agent via runner
        final_response_text = "Agent did not produce a final response."
        
        async for event in runner.run_async(
            user_id=user_id,
            session_id=session_id,
            new_message=content
        ):
            # Check for final response
            if event.is_final_response():
                if event.content and event.content.parts:
                    final_response_text = event.content.parts[0].text
                    break
        
        logger.info(f"Agent response generated successfully")
        
        print(f"\nAgent: {final_response_text}")
        print("-" * 50)
        
    except Exception as e:
        logger.error(f"Error processing query '{query}': {str(e)}", exc_info=True)
        print(f"\nSorry, I encountered an error processing your query: {str(e)}")
        print("Please try again with a different query.")
        print("-" * 50)


def main():
    """Main application loop."""
    async def async_main():
        # Setup logging
        logger = setup_logging()
        logger.info("Starting LLM ES Agent application")
        
        try:
            # Initialize orchestrator agent
            logger.info("Initializing Orchestrator Agent")
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
                session_service=session_service
            )
            
            logger.info("ADK Runner initialized successfully")
            
            # Print welcome message
            print_welcome_message()
            
            # Main interaction loop
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
                    if user_input.lower() in ['quit', 'exit', 'q']:
                        print("Goodbye!")
                        logger.info("User requested exit")
                        break
                    elif user_input.lower() in ['help', 'h']:
                        print_help()
                        continue
                    
                    # Process regular query
                    await process_user_query(runner, session_service, user_input, USER_ID, APP_NAME, logger)
                    
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
    
    # Run the async main function
    asyncio.run(async_main())


if __name__ == "__main__":
    main()