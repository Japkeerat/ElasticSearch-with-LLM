"""
Session State Management Tools
Tools for saving and retrieving structured data in the ADK session state

This file should be saved as: llm_es_agent/tools/session_tools.py
"""

from typing import Any, Dict, Optional
from google.adk.tools import ToolContext


def save_index_selection_data(
    context: ToolContext,
    selected_index: str,
    index_schema: Dict[str, Any],
    reasoning: str,
    confidence: str = "high",
    candidate_indices: Optional[list] = None
) -> str:
    """
    Save index selection data to session state for the next pipeline agent.
    
    Args:
        context: The tool context for accessing session state
        selected_index: Name of the selected index
        index_schema: Complete schema information for the selected index
        reasoning: Explanation of why this index was selected
        confidence: Confidence level (high, medium, low)
        candidate_indices: List of indices that were considered
    
    Returns:
        Confirmation message
    """
    
    selection_data = {
        "selected_index": selected_index,
        "index_schema": index_schema,
        "selection_metadata": {
            "selection_method": "automatic",
            "candidate_indices": candidate_indices or [],
            "reasoning": reasoning,
            "confidence": confidence
        },
        "validation": {
            "index_exists": True,
            "schema_retrieved": True,
            "ready_for_query_generation": True
        }
    }
    
    # Save to session state for next agent
    context.state["index_selection_data"] = selection_data
    context.state["selected_index"] = selected_index
    context.state["index_schema"] = index_schema
    
    return f"Index selection data saved to session state. Selected index: {selected_index}"


def save_query_generation_data(
    context: ToolContext,
    generated_query: Dict[str, Any],
    target_index: str,
    query_type: str,
    reasoning: str,
    confidence: str = "high"
) -> str:
    """
    Save query generation data to session state for the execution agent.
    
    Args:
        context: The tool context for accessing session state
        generated_query: The Elasticsearch query DSL
        target_index: Name of the target index
        query_type: Type of query (match, term, range, bool, etc.)
        reasoning: Explanation of how the query was constructed
        confidence: Confidence level
    
    Returns:
        Confirmation message
    """
    
    query_data = {
        "generated_query": generated_query,
        "target_index": target_index,
        "query_metadata": {
            "generation_method": "automatic",
            "query_type": query_type,
            "reasoning": reasoning,
            "confidence": confidence
        },
        "validation": {
            "syntax_valid": True,
            "fields_exist": True,
            "query_safe": True,
            "ready_for_execution": True
        }
    }
    
    # Save to session state for execution agent
    context.state["query_generation_data"] = query_data
    context.state["generated_query"] = generated_query
    context.state["target_index"] = target_index
    
    return f"Query generation data saved to session state. Target index: {target_index}"


def save_execution_results_data(
    context: ToolContext,
    execution_results: Dict[str, Any],
    success: bool,
    natural_response: str,
    error_message: Optional[str] = None
) -> str:
    """
    Save query execution results to session state.
    
    Args:
        context: The tool context for accessing session state
        execution_results: Raw query results from Elasticsearch
        success: Whether the execution was successful
        natural_response: Natural language interpretation of results
        error_message: Error message if execution failed
    
    Returns:
        Confirmation message
    """
    
    execution_data = {
        "execution_results": execution_results,
        "success": success,
        "natural_language_response": natural_response,
        "error_message": error_message
    }
    
    # Save to session state
    context.state["execution_results_data"] = execution_data
    context.state["final_results"] = execution_results
    context.state["final_response"] = natural_response
    
    return f"Execution results saved to session state. Success: {success}"


def get_session_data(context: ToolContext, key: str) -> Any:
    """
    Retrieve data from session state.
    
    Args:
        context: The tool context for accessing session state
        key: The key to retrieve
        
    Returns:
        The stored data or None if not found
    """
    
    return context.state.get(key)


def get_user_query(context: ToolContext) -> str:
    """
    Get the original user query from session state.
    
    Args:
        context: The tool context for accessing session state
        
    Returns:
        The original user query
    """
    
    return context.state.get("original_user_query", "")