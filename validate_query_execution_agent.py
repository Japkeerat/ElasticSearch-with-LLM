#!/usr/bin/env python3
"""
Simple validation script for the Query Execution Agent.
This script validates that the agent can be created and basic functionality works.
"""

import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


def test_imports():
    """Test that all imports work correctly."""
    print("Testing imports...")

    try:
        from llm_es_agent.agents.query_execution_agent import (
            create_query_execution_agent,
        )

        print("✅ Query Execution Agent import successful")

        from llm_es_agent.pipeline_agent import create_elasticsearch_pipeline_agent

        print("✅ Pipeline Agent import successful")

        from llm_es_agent.tools.elasticsearch_tools import QueryExecutionTools

        print("✅ Query Execution Tools import successful")

        return True
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error during import: {e}")
        return False


def test_agent_creation():
    """Test that agents can be created without errors."""
    print("\nTesting agent creation...")

    try:
        from llm_es_agent.agents.query_execution_agent import (
            create_query_execution_agent,
        )
        from llm_es_agent.pipeline_agent import create_elasticsearch_pipeline_agent

        # Test Query Execution Agent creation
        execution_agent = create_query_execution_agent()
        print("✅ Query Execution Agent created successfully")
        print(f"   Agent name: {execution_agent.agent.name}")
        print(f"   Agent description: {execution_agent.agent.description}")

        # Test Pipeline Agent creation
        pipeline_agent = create_elasticsearch_pipeline_agent()
        print("✅ Pipeline Agent created successfully")
        print(f"   Agent name: {pipeline_agent.agent.name}")
        print(f"   Agent description: {pipeline_agent.agent.description}")
        print(f"   Sub-agents count: {len(pipeline_agent.agent.sub_agents)}")

        return True
    except Exception as e:
        print(f"❌ Agent creation failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_tools_creation():
    """Test that tools can be created and basic methods work."""
    print("\nTesting tools creation...")

    try:
        from llm_es_agent.tools.elasticsearch_tools import QueryExecutionTools

        # Create tools instance
        tools = QueryExecutionTools()
        print("✅ Query Execution Tools created successfully")

        # Test basic tool methods (without actual ES connection)
        test_query = {"query": {"match_all": {}}}

        # Test read-only validation
        is_readonly = tools._is_read_only_query(test_query)
        print(f"✅ Read-only validation works: {is_readonly}")

        # Test error suggestions
        suggestions = tools._get_error_suggestions("index not found")
        print(f"✅ Error suggestions work: {len(suggestions)} suggestions")

        # Test key field extraction
        test_doc = {"title": "Test", "date": "2024-03-20", "content": "Sample content"}
        key_fields = tools._extract_key_fields(test_doc)
        print(f"✅ Key field extraction works: {list(key_fields.keys())}")

        # Verify the main execute_query method exists
        if hasattr(tools, "execute_query"):
            print("✅ execute_query method exists")
        else:
            print("❌ execute_query method missing")
            return False

        return True
    except Exception as e:
        print(f"❌ Tools testing failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_prompt_files():
    """Test that prompt files exist and can be read."""
    print("\nTesting prompt files...")

    try:
        # Test Query Execution Agent prompt
        execution_prompt_path = Path("prompts/query_execution_agent.txt")
        if execution_prompt_path.exists():
            with open(execution_prompt_path, "r") as f:
                content = f.read()
            print(f"✅ Query Execution Agent prompt exists ({len(content)} characters)")
        else:
            print("❌ Query Execution Agent prompt file not found")
            return False

        # Test Pipeline Agent prompt
        pipeline_prompt_path = Path("prompts/elasticsearch_pipeline_agent.txt")
        if pipeline_prompt_path.exists():
            with open(pipeline_prompt_path, "r") as f:
                content = f.read()
            print(f"✅ Pipeline Agent prompt exists ({len(content)} characters)")
        else:
            print("❌ Pipeline Agent prompt file not found")
            return False

        return True
    except Exception as e:
        print(f"❌ Prompt file testing failed: {e}")
        return False


def main():
    """Run all validation tests."""
    print("🧪 Query Execution Agent Validation Suite")
    print("=" * 50)

    tests = [
        ("Import Tests", test_imports),
        ("Agent Creation Tests", test_agent_creation),
        ("Tools Tests", test_tools_creation),
        ("Prompt Files Tests", test_prompt_files),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        print(f"\n📋 {test_name}")
        print("-" * 30)
        if test_func():
            passed += 1
            print(f"✅ {test_name} PASSED")
        else:
            print(f"❌ {test_name} FAILED")

    print("\n" + "=" * 50)
    print("VALIDATION SUMMARY")
    print("=" * 50)
    print(f"Tests passed: {passed}/{total}")

    if passed == total:
        print("🎉 All validation tests PASSED!")
        print("The Query Execution Agent is ready for use.")
        return True
    else:
        print("⚠️  Some validation tests FAILED.")
        print("Please check the errors above before using the agent.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
