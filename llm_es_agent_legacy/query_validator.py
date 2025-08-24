def is_query_safe(query: dict) -> bool:
    # Ensure it only contains 'query', 'size', 'aggs', etc.
    allowed_keys = {"query", "aggs", "size", "sort", "from", "_source", "aggregations",}
    return all(key in allowed_keys for key in query.keys())
