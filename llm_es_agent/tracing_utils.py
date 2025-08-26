"""
Tracing utilities to handle OpenTelemetry context issues in async environments.

This module provides utilities to safely handle OpenTelemetry tracing in async environments
where context detachment errors commonly occur, especially with streaming/generator patterns
used by Google ADK and similar frameworks.
"""

import logging
import warnings
import os
from contextlib import contextmanager
from typing import Optional

logger = logging.getLogger(__name__)


def suppress_otel_context_warnings():
    """
    Suppress OpenTelemetry context warnings that occur in async environments.
    
    This function filters out specific OpenTelemetry context warnings and errors
    that commonly occur when using async generators and streaming patterns.
    """
    # Filter out the specific OpenTelemetry context warnings
    warnings.filterwarnings(
        "ignore",
        message=".*Failed to detach context.*",
        category=UserWarning,
        module="opentelemetry.*"
    )
    
    # Also suppress at the logging level
    otel_logger = logging.getLogger("opentelemetry.context")
    otel_logger.setLevel(logging.CRITICAL)
    
    # Suppress the specific ValueError that occurs with context detachment
    otel_trace_logger = logging.getLogger("opentelemetry.trace")
    otel_trace_logger.setLevel(logging.CRITICAL)
    
    # Suppress other related OpenTelemetry loggers
    otel_sdk_logger = logging.getLogger("opentelemetry.sdk")
    otel_sdk_logger.setLevel(logging.CRITICAL)


def setup_safe_tracing_environment():
    """
    Set up a safe tracing environment that handles context issues gracefully.
    
    This should be called early in the application startup to configure
    environment variables and suppress problematic warnings.
    """
    # Set environment variables for better context handling
    os.environ.setdefault("OTEL_PYTHON_CONTEXT", "contextvars_context")
    os.environ.setdefault("OTEL_PYTHON_LOG_CORRELATION", "false")
    
    # Disable automatic instrumentation that can cause issues
    os.environ.setdefault("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", "")
    
    # Suppress warnings
    suppress_otel_context_warnings()
    
    logger.info("Safe tracing environment configured")


@contextmanager
def safe_tracing_context():
    """
    Context manager that safely handles OpenTelemetry tracing errors.
    
    This prevents context detachment errors from propagating and crashing the application.
    Use this around code that might trigger OpenTelemetry context issues, especially
    async generators and streaming operations.
    
    Example:
        with safe_tracing_context():
            async for event in some_async_generator():
                process_event(event)
    """
    try:
        yield
    except Exception as e:
        error_msg = str(e).lower()
        if any(keyword in error_msg for keyword in ["context", "token", "detach"]):
            # Silently ignore context-related errors
            logger.debug(f"Suppressed OpenTelemetry context error: {e}")
        else:
            # Re-raise non-context related errors
            raise


class SafeTracer:
    """
    A wrapper around OpenTelemetry tracer that handles context errors gracefully.
    
    This tracer wrapper provides the same interface as a regular OpenTelemetry tracer
    but catches and handles context-related errors that can occur in async environments.
    """
    
    def __init__(self, tracer: Optional[object] = None):
        """
        Initialize the SafeTracer.
        
        Args:
            tracer: The underlying OpenTelemetry tracer instance, or None to disable tracing
        """
        self.tracer = tracer
        self.enabled = tracer is not None
    
    def start_as_current_span(self, name: str, **kwargs):
        """
        Start a span with safe context handling.
        
        Args:
            name: The span name
            **kwargs: Additional arguments passed to the underlying tracer
            
        Returns:
            A span object (either real or dummy)
        """
        if not self.enabled:
            return self._dummy_span()
        
        try:
            return self.tracer.start_as_current_span(name, **kwargs)
        except Exception as e:
            logger.debug(f"Failed to start span '{name}': {e}")
            return self._dummy_span()
    
    def _dummy_span(self):
        """Return a dummy span that does nothing."""
        return DummySpan()


class DummySpan:
    """
    A dummy span that implements the span interface but does nothing.
    
    This is used as a fallback when real tracing fails, ensuring that
    application code doesn't break when tracing is unavailable.
    """
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    
    def set_attribute(self, key: str, value) -> None:
        """Set an attribute on the span (no-op)."""
        pass
    
    def add_event(self, name: str, attributes=None) -> None:
        """Add an event to the span (no-op)."""
        pass
    
    def set_status(self, status, description: str = None) -> None:
        """Set the span status (no-op)."""
        pass
    
    def record_exception(self, exception: Exception) -> None:
        """Record an exception on the span (no-op)."""
        pass


def create_safe_tracer(tracer_name: str = __name__) -> SafeTracer:
    """
    Create a SafeTracer instance.
    
    This function attempts to create a real OpenTelemetry tracer and wraps it
    in a SafeTracer. If OpenTelemetry is not available, returns a SafeTracer
    with no underlying tracer (which will use dummy spans).
    
    Args:
        tracer_name: The name for the tracer
        
    Returns:
        A SafeTracer instance
    """
    try:
        from opentelemetry import trace as trace_api
        tracer = trace_api.get_tracer(tracer_name)
        return SafeTracer(tracer)
    except ImportError:
        logger.debug("OpenTelemetry not available, using dummy tracer")
        return SafeTracer(None)
    except Exception as e:
        logger.debug(f"Failed to create tracer: {e}")
        return SafeTracer(None)


def patch_adk_context_handling():
    """
    Apply patches to Google ADK to handle context issues better.
    
    This function applies monkey patches to common problematic areas
    in the Google ADK framework to prevent context detachment errors.
    """
    try:
        # Try to patch the ADK runners module if available
        import google.adk.runners
        
        # Store original methods
        if not hasattr(google.adk.runners, '_original_run_with_trace'):
            google.adk.runners._original_run_with_trace = getattr(
                google.adk.runners, '_run_with_trace', None
            )
        
        # Define patched version
        def safe_run_with_trace(*args, **kwargs):
            with safe_tracing_context():
                if google.adk.runners._original_run_with_trace:
                    return google.adk.runners._original_run_with_trace(*args, **kwargs)
                else:
                    # Fallback if method doesn't exist
                    return None
        
        # Apply patch if the method exists
        if hasattr(google.adk.runners, '_run_with_trace'):
            google.adk.runners._run_with_trace = safe_run_with_trace
            logger.debug("Applied ADK context handling patch")
        
    except ImportError:
        logger.debug("Google ADK not available, skipping patches")
    except Exception as e:
        logger.debug(f"Failed to apply ADK patches: {e}")


def initialize_safe_tracing(service_name: str = "llm-es-agent", 
                          phoenix_endpoint: str = "http://localhost:6006") -> bool:
    """
    Initialize safe tracing with Phoenix observability.
    
    This is a convenience function that sets up the complete tracing environment
    including safe context handling and Phoenix integration.
    
    Args:
        service_name: The service name for tracing
        phoenix_endpoint: Phoenix server endpoint URL
        
    Returns:
        bool: True if tracing was set up successfully
    """
    try:
        # Set up safe environment first
        setup_safe_tracing_environment()
        
        # Apply ADK patches
        patch_adk_context_handling()
        
        # Try to set up Phoenix tracing
        from opentelemetry import trace as trace_api
        from opentelemetry.sdk import trace as trace_sdk
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        
        resource = Resource.create({"service.name": service_name})
        tracer_provider = trace_sdk.TracerProvider(resource=resource)
        trace_api.set_tracer_provider(tracer_provider)
        
        otlp_exporter = OTLPSpanExporter(
            endpoint=f"{phoenix_endpoint}/v1/traces", 
            headers={}
        )
        tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
        
        logger.info(f"✅ Safe tracing initialized - Dashboard: {phoenix_endpoint}")
        return True
        
    except ImportError:
        logger.info("OpenTelemetry/Phoenix not available, tracing disabled")
        return False
    except Exception as e:
        logger.warning(f"Could not initialize tracing: {str(e)}")
        return False
