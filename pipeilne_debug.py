#!/usr/bin/env python3
"""
Pipeline Debug Script
Debug why the SequentialAgent pipeline stops after the first agent
"""

import sys
import os
import asyncio
import logging
from pathlib import Path

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

from llm_es_agent.orchestrator import create_orchestrator
from llm_es_agent.pipeline_agent import create_elasticsearch_pipeline_agent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from dotenv import load_dotenv

load_dotenv()

# Set up logging to see what's happening
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

async def debug_pipeline():
    """Debug the pipeline step by step"""
    
    print("🔍 Pipeline Debug Analysis")
    print("=" * 50)
    
    # 1. Test individual agents first
    print("\n1️⃣ Testing Individual Agents:")
    print("-" * 30)
    
    try:
        pipeline_agent = create_elasticsearch_pipeline_agent()
        print(f"✅ Pipeline agent created: {pipeline_agent.agent.name}")
        
        # Check sub-agents
        if hasattr(pipeline_agent.agent, 'sub_agents'):
            sub_agents = pipeline_agent.agent.sub_agents
            print(f"📋 Sub-agents configured: {len(sub_agents)}")
            for i, agent in enumerate(sub_agents):
                print(f"   {i+1}. {agent.name} - {type(agent).__name__}")
        else:
            print("❌ No sub_agents found on SequentialAgent")
            return False
            
    except Exception as e:
        print(f"❌ Failed to create pipeline agent: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # 2. Test orchestrator
    print("\n2️⃣ Testing Orchestrator:")
    print("-" * 30)
    
    try:
        orchestrator = create_orchestrator()
        print(f"✅ Orchestrator created: {orchestrator.agent.name}")
    except Exception as e:
        print(f"❌ Failed to create orchestrator: {e}")
        return False
    
    # 3. Test session and runner setup
    print("\n3️⃣ Testing Session & Runner Setup:")
    print("-" * 30)
    
    try:
        session_service = InMemorySessionService()
        runner = Runner(
            agent=orchestrator.agent,
            app_name="pipeline_debug",
            session_service=session_service,
        )
        print("✅ Session service and runner created successfully")
    except Exception as e:
        print(f"❌ Failed to create session/runner: {e}")
        return False
    
    # 4. Test a simple query with detailed logging
    print("\n4️⃣ Testing Query Processing with Debug Info:")
    print("-" * 30)
    
    try:
        # Create session
        session = await session_service.create_session(
            app_name="pipeline_debug",
            user_id="debug_user",
            session_id="debug_session",
            state={"original_user_query": "how many users exist?"}
        )
        print("✅ Session created successfully")
        
        content = types.Content(role="user", parts=[types.Part(text="how many users exist?")])
        
        print("\n📝 Processing events:")
        event_count = 0
        agent_responses = []
        
        async for event in runner.run_async(
            user_id="debug_user",
            session_id="debug_session",
            new_message=content
        ):
            event_count += 1
            event_type = type(event).__name__
            
            print(f"Event {event_count}: {event_type}")
            
            # Log event details
            if hasattr(event, 'author'):
                print(f"  Author: {event.author}")
            
            if hasattr(event, 'content') and event.content:
                if event.content.parts:
                    content_text = event.content.parts[0].text[:100]
                    print(f"  Content: {content_text}...")
                    agent_responses.append({
                        'author': getattr(event, 'author', 'unknown'),
                        'content': event.content.parts[0].text[:200]
                    })
            
            if hasattr(event, 'actions') and event.actions:
                print(f"  Actions: {event.actions}")
            
            # Check if this is the final response
            if hasattr(event, 'is_final_response') and event.is_final_response():
                print("  🎯 This is marked as final response")
                break
            
            # Safety check
            if event_count > 20:
                print("  ⚠️ Breaking after 20 events for safety")
                break
        
        print(f"\n📊 Summary:")
        print(f"Total events: {event_count}")
        print(f"Agent responses: {len(agent_responses)}")
        
        for i, response in enumerate(agent_responses):
            print(f"\nResponse {i+1} from {response['author']}:")
            print(f"  {response['content']}")
        
        return len(agent_responses) >= 3  # Should have responses from all 3 agents
        
    except Exception as e:
        print(f"❌ Error during query processing: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_individual_agent():
    """Test just the index selection agent by itself"""
    
    print("\n🔬 Testing Index Selection Agent Individually:")
    print("=" * 50)
    
    try:
        from llm_es_agent.agents.index_selection_agent import create_index_selection_agent
        
        index_agent = create_index_selection_agent()
        print(f"✅ Index agent created: {index_agent.agent.name}")
        
        # Test with session service
        session_service = InMemorySessionService()
        runner = Runner(
            agent=index_agent.agent,
            app_name="index_test",
            session_service=session_service,
        )
        
        session = await session_service.create_session(
            app_name="index_test",
            user_id="test_user",
            session_id="test_session",
            state={"original_user_query": "how many users exist?"}
        )
        
        content = types.Content(role="user", parts=[types.Part(text="how many users exist?")])
        
        print("Processing with individual agent:")
        async for event in runner.run_async(
            user_id="test_user",
            session_id="test_session",
            new_message=content
        ):
            if hasattr(event, 'content') and event.content and event.content.parts:
                response = event.content.parts[0].text
                print(f"Individual agent response: {response[:200]}...")
                break
        
    except Exception as e:
        print(f"❌ Error testing individual agent: {e}")
        import traceback
        traceback.print_exc()

def check_agent_configuration():
    """Check the current agent configuration"""
    
    print("\n⚙️ Agent Configuration Check:")
    print("=" * 50)
    
    try:
        # Check if session tools exist
        session_tools_path = Path("llm_es_agent/tools/session_tools.py")
        if session_tools_path.exists():
            print("✅ session_tools.py exists")
        else:
            print("❌ session_tools.py is missing!")
            print(f"   Expected at: {session_tools_path.absolute()}")
        
        # Check individual agents
        from llm_es_agent.agents.index_selection_agent import create_index_selection_agent
        from llm_es_agent.agents.query_generation_agent import create_query_generation_agent
        from llm_es_agent.agents.query_execution_agent import create_query_execution_agent
        
        agents = [
            ("Index Selection", create_index_selection_agent),
            ("Query Generation", create_query_generation_agent),
            ("Query Execution", create_query_execution_agent)
        ]
        
        for name, create_func in agents:
            try:
                agent_obj = create_func()
                agent = agent_obj.agent
                
                print(f"✅ {name} Agent:")
                print(f"   Name: {agent.name}")
                print(f"   Tools: {len(getattr(agent, 'tools', []))}")
                print(f"   Has output_schema: {hasattr(agent, 'output_schema') and agent.output_schema is not None}")
                
                # Check if tools include session management
                tool_names = [tool.function.__name__ if hasattr(tool, 'function') else str(tool) 
                            for tool in getattr(agent, 'tools', [])]
                has_session_tools = any('session' in name.lower() for name in tool_names)
                print(f"   Has session tools: {has_session_tools}")
                
            except Exception as e:
                print(f"❌ {name} Agent failed: {e}")
        
    except Exception as e:
        print(f"❌ Configuration check failed: {e}")

async def main():
    """Main debug function"""
    
    print("🚀 Starting Pipeline Debug Session")
    print("Time:", os.popen('date').read().strip())
    print()
    
    # Run all debug checks
    check_agent_configuration()
    await test_individual_agent()
    success = await debug_pipeline()
    
    print("\n" + "=" * 50)
    if success:
        print("✅ Pipeline appears to be working correctly!")
    else:
        print("❌ Pipeline has issues that need to be resolved.")
        print("\n💡 Common solutions:")
        print("1. Ensure all agent files are updated with the new code")
        print("2. Verify session_tools.py exists and is importable")
        print("3. Check that agents don't have output_schema set")
        print("4. Restart the Docker containers after code changes")

if __name__ == "__main__":
    asyncio.run(main())