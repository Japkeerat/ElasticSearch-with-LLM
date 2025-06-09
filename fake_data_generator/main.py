from data_generator import generate_bulk_logs
from es_client import setup_index, bulk_insert


if __name__ == "__main__":
    setup_index()
    logs = generate_bulk_logs(10000)
    bulk_insert(logs)
    print(f"✅ Inserted {len(logs)} synthetic audit logs.")
