from langchain.prompts import PromptTemplate
from langchain_openai.chat_models.base import ChatOpenAI
import json
import re
import os

from dotenv import load_dotenv

load_dotenv()

# Make sure to set your OPENAI_API_KEY in environment variables
# export OPENAI_API_KEY="your_openai_api_key"

llm = ChatOpenAI(
    # openai_api_key="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",
    model="gpt-4o-mini",  # or "gpt-3.5-turbo" or whichever you prefer
    temperature=0,  # deterministic outputs, better for query generation
    max_tokens=1000,
)

prompt_template = PromptTemplate(
    input_variables=["question", "schema"],
    template="""
    You are an expert Elasticsearch query builder.

    Given the following Elasticsearch schema:
    {schema}

    Generate ONLY the JSON Elasticsearch DSL query that answers:
    "{question}"

    Do NOT include any explanation or text. Just output valid JSON.
"""
)


def extract_json(text):
    try:
        # First, remove markdown code blocks if present
        # This regex extracts content inside ```json ... ```
        code_block_match = re.search(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL)
        if code_block_match:
            json_str = code_block_match.group(1)
            return json.loads(json_str)

        # Fallback: try direct load if no markdown block
        return json.loads(text)

    except json.JSONDecodeError:
        # Fallback: extract first JSON-like block using a more generic regex
        match = re.search(r'(\{.*\})', text, re.DOTALL)
        if match:
            return json.loads(match.group(1))
        raise ValueError("Failed to extract valid JSON from LLM output.")


def generate_es_query(question, schema_path="elasticsearch_schema.json"):
    with open(schema_path) as f:
        schema = json.load(f)

    prompt = prompt_template.format(question=question, schema=json.dumps(schema))
    print(f"Generating ES query for {question} with prompt: {prompt}")

    messages = [
        ("human", prompt)
    ]

    response = llm.invoke(messages)
    response.text()
    print(f"response received: {response.text()}")

    # In case there's some artifact tags (like your original </think>), clean them
    # if "</think>" in response:
    #     response = response.split("</think>")[-1].strip()

    try:
        json_content = extract_json(response.text())
        print(json.dumps(json_content, indent=2))
        return json_content
    except Exception:
        raise ValueError("Failed to parse valid ES query from LLM.")
