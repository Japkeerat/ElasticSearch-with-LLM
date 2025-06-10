# response_formatter.py

import json
from langchain_community.llms import Ollama

# Initialize your small LLM (you can replace with another if needed)
llm = Ollama(model="llama3.2:latest")

def format_response(question: str, query_result: dict) -> str:
    prompt = f"""
The user asked: "{question}"

Elasticsearch returned this JSON result:
{query_result}

Convert this into a simple, human-readable natural language answer.
Keep it concise, and only include relevant information.
"""
    return llm.invoke(prompt)
