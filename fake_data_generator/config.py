INDEX_NAME = "audit-logs"

INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "log_id": {"type": "keyword"},
            "timestamp": {"type": "date"},
            "user": {
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {"type": "keyword"},
                    "role": {"type": "keyword"}
                }
            },
            "session": {
                "properties": {
                    "login_time": {"type": "date"},
                    "logout_time": {"type": "date"}
                }
            },
            "action": {"type": "keyword"},
            "entity": {
                "properties": {
                    "type": {"type": "keyword"},
                    "id": {"type": "keyword"},
                    "field_changes": {
                        "type": "nested",
                        "properties": {
                            "field": {"type": "keyword"},
                            "old": {"type": "text"},
                            "new": {"type": "text"}
                        }
                    }
                }
            },
            "client_ip": {"type": "ip"}
        }
    }
}
