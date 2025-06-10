# langchain_agent.py
from query_generator import generate_es_query
from query_validator import is_query_safe
from query_executor import run_query
from response_formatter import format_response

def answer_question(user_question):
    print("🔍 Analyzing query...")
    query = generate_es_query(user_question)

    # if not is_query_safe(query):
    #     return "⚠️ Query is unsafe or uses forbidden operations."
    #
    # print("✅ Query validated. Running against Elasticsearch...")
    result = run_query(query)

    print(f"🧠 Formatting result... {result}")

    return format_response(user_question, result)
