#!/usr/bin/env python3
"""
Reliable Elasticsearch Data Generator
This version focuses on actually getting data into Elasticsearch successfully
"""

import json
import requests
from faker import Faker
import random
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import time

load_dotenv()

def generate_and_insert_data():
    """Generate and insert data with better error handling and verification"""
    
    es_url = os.getenv('ES_HOST', 'http://localhost:9200').rstrip('/')
    fake = Faker('en_US')
    
    print(f"🚀 Reliable Data Generator")
    print(f"🔗 Target: {es_url}")
    print("=" * 50)
    
    # Test connection first
    try:
        response = requests.get(f"{es_url}/_cluster/health", timeout=10)
        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code}: {response.text}")
        
        health = response.json()
        print(f"✅ Connected to Elasticsearch")
        print(f"📊 Cluster: {health['cluster_name']} ({health['status']})")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

    # Delete existing indices to start fresh
    indices_to_create = ['users', 'error_logs', 'user_activities', 'system_metrics', 'orders']
    
    for index in indices_to_create:
        try:
            response = requests.delete(f"{es_url}/{index}")
            if response.status_code in [200, 404]:
                print(f"🗑️  Cleared index: {index}")
        except Exception as e:
            print(f"⚠️  Could not clear {index}: {e}")
    
    # Wait a bit for deletions to complete
    time.sleep(2)
    
    # Generate and insert users
    print("\n👥 Creating users data...")
    users_inserted = create_users_index(es_url, fake)
    
    # Generate and insert error logs  
    print("\n🚨 Creating error logs data...")
    errors_inserted = create_error_logs_index(es_url, fake)
    
    # Generate and insert user activities
    print("\n📊 Creating user activities data...")
    activities_inserted = create_activities_index(es_url, fake)
    
    # Generate and insert system metrics
    print("\n⚡ Creating system metrics data...")
    metrics_inserted = create_metrics_index(es_url, fake)
    
    # Generate and insert orders
    print("\n🛒 Creating orders data...")
    orders_inserted = create_orders_index(es_url, fake)
    
    # Final verification
    print("\n" + "=" * 50)
    print("📊 Final Results:")
    print(f"👥 Users: {users_inserted}")
    print(f"🚨 Error Logs: {errors_inserted}")
    print(f"📊 Activities: {activities_inserted}")
    print(f"⚡ Metrics: {metrics_inserted}")
    print(f"🛒 Orders: {orders_inserted}")
    
    total = users_inserted + errors_inserted + activities_inserted + metrics_inserted + orders_inserted
    print(f"📈 Total Documents: {total}")
    
    if total > 0:
        print("\n✅ Data generation successful!")
        print("💡 You can now test queries like:")
        print("   • How many users are in the system?")
        print("   • Show me recent error logs")
        print("   • What are the top 10 most active users?")
    else:
        print("\n❌ No data was inserted. Check the errors above.")
    
    return total > 0

def create_users_index(es_url, fake, count=100):
    """Create users index with actual data"""
    
    # Create index with mapping
    mapping = {
        "mappings": {
            "properties": {
                "user_id": {"type": "keyword"},
                "username": {"type": "keyword"},
                "full_name": {"type": "text"},
                "email": {"type": "keyword"},
                "status": {"type": "keyword"},
                "login_count": {"type": "integer"},
                "created_date": {"type": "date"},
                "last_login": {"type": "date"},
                "age": {"type": "integer"},
                "country": {"type": "keyword"}
            }
        }
    }
    
    try:
        response = requests.put(f"{es_url}/users", json=mapping)
        if response.status_code not in [200, 201]:
            print(f"⚠️  Index creation warning: {response.status_code}")
    except Exception as e:
        print(f"❌ Could not create users mapping: {e}")
    
    # Generate and insert users one by one for reliability
    inserted_count = 0
    
    for i in range(count):
        user = {
            'user_id': f"user_{i+1:04d}",
            'username': fake.user_name(),
            'full_name': fake.name(),
            'email': fake.email(),
            'status': random.choice(['active', 'inactive', 'suspended']),
            'login_count': random.randint(1, 500),
            'created_date': fake.date_time_between(start_date='-1y').isoformat(),
            'last_login': fake.date_time_between(start_date='-7d').isoformat(),
            'age': random.randint(18, 65),
            'country': fake.country()
        }
        
        try:
            response = requests.post(
                f"{es_url}/users/_doc",
                json=user,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code in [200, 201]:
                inserted_count += 1
            else:
                if i < 5:  # Only print first few errors to avoid spam
                    print(f"⚠️  Failed to insert user {i+1}: {response.status_code}")
                
        except Exception as e:
            if i < 5:
                print(f"❌ Error inserting user {i+1}: {e}")
    
    # Refresh index to make documents searchable
    try:
        requests.post(f"{es_url}/users/_refresh")
    except:
        pass
    
    print(f"   ✅ Inserted {inserted_count}/{count} users")
    return inserted_count

def create_error_logs_index(es_url, fake, count=200):
    """Create error logs index"""
    
    mapping = {
        "mappings": {
            "properties": {
                "timestamp": {"type": "date"},
                "level": {"type": "keyword"},
                "service": {"type": "keyword"},
                "message": {"type": "text"},
                "error_type": {"type": "keyword"},
                "resolved": {"type": "boolean"},
                "user_id": {"type": "keyword"}
            }
        }
    }
    
    requests.put(f"{es_url}/error_logs", json=mapping)
    
    inserted_count = 0
    error_levels = ['ERROR', 'CRITICAL', 'WARNING']
    services = ['user-service', 'payment-service', 'auth-service', 'api-gateway']
    error_types = ['DatabaseError', 'ValidationError', 'TimeoutError', 'NetworkError']
    
    for i in range(count):
        error = {
            'timestamp': fake.date_time_between(start_date='-7d').isoformat(),
            'level': random.choice(error_levels),
            'service': random.choice(services),
            'error_type': random.choice(error_types),
            'message': f"{random.choice(error_types)} in {random.choice(services)}: {fake.sentence()}",
            'resolved': fake.boolean(chance_of_getting_true=70),
            'user_id': f"user_{random.randint(1, 100):04d}" if random.random() > 0.3 else None
        }
        
        try:
            response = requests.post(f"{es_url}/error_logs/_doc", json=error)
            if response.status_code in [200, 201]:
                inserted_count += 1
        except Exception as e:
            if i < 5:
                print(f"❌ Error inserting log {i+1}: {e}")
    
    requests.post(f"{es_url}/error_logs/_refresh")
    print(f"   ✅ Inserted {inserted_count}/{count} error logs")
    return inserted_count

def create_activities_index(es_url, fake, count=500):
    """Create user activities index with realistic activity patterns"""
    
    mapping = {
        "mappings": {
            "properties": {
                "timestamp": {"type": "date"},
                "user_id": {"type": "keyword"},
                "activity_type": {"type": "keyword"},
                "session_id": {"type": "keyword"},
                "page_url": {"type": "keyword"},
                "device_type": {"type": "keyword"}
            }
        }
    }
    
    requests.put(f"{es_url}/user_activities", json=mapping)
    
    inserted_count = 0
    
    # Create realistic activity distribution - some users are much more active
    active_users = [f"user_{i:04d}" for i in range(1, 21)]  # Top 20 most active
    regular_users = [f"user_{i:04d}" for i in range(21, 101)]  # Regular users
    
    activity_types = ['login', 'logout', 'page_view', 'click', 'search', 'purchase']
    device_types = ['mobile', 'desktop', 'tablet']
    pages = ['/home', '/dashboard', '/profile', '/products', '/search', '/checkout']
    
    for i in range(count):
        # 70% of activities from top active users
        if random.random() < 0.7:
            user_id = random.choice(active_users)
        else:
            user_id = random.choice(regular_users)
        
        activity = {
            'timestamp': fake.date_time_between(start_date='-7d').isoformat(),
            'user_id': user_id,
            'activity_type': random.choice(activity_types),
            'session_id': fake.uuid4()[:8],
            'page_url': random.choice(pages),
            'device_type': random.choice(device_types)
        }
        
        try:
            response = requests.post(f"{es_url}/user_activities/_doc", json=activity)
            if response.status_code in [200, 201]:
                inserted_count += 1
        except Exception as e:
            if i < 5:
                print(f"❌ Error inserting activity {i+1}: {e}")
    
    requests.post(f"{es_url}/user_activities/_refresh")
    print(f"   ✅ Inserted {inserted_count}/{count} user activities")
    return inserted_count

def create_metrics_index(es_url, fake, count=100):
    """Create system metrics index"""
    
    mapping = {
        "mappings": {
            "properties": {
                "timestamp": {"type": "date"},
                "service": {"type": "keyword"},
                "cpu_usage": {"type": "float"},
                "memory_usage": {"type": "float"},
                "response_time_ms": {"type": "integer"}
            }
        }
    }
    
    requests.put(f"{es_url}/system_metrics", json=mapping)
    
    inserted_count = 0
    services = ['web-server', 'database', 'cache', 'api-gateway']
    
    for i in range(count):
        metric = {
            'timestamp': fake.date_time_between(start_date='-7d').isoformat(),
            'service': random.choice(services),
            'cpu_usage': round(random.uniform(10, 95), 2),
            'memory_usage': round(random.uniform(20, 90), 2),
            'response_time_ms': random.randint(50, 5000)
        }
        
        try:
            response = requests.post(f"{es_url}/system_metrics/_doc", json=metric)
            if response.status_code in [200, 201]:
                inserted_count += 1
        except Exception as e:
            if i < 5:
                print(f"❌ Error inserting metric {i+1}: {e}")
    
    requests.post(f"{es_url}/system_metrics/_refresh")
    print(f"   ✅ Inserted {inserted_count}/{count} system metrics")
    return inserted_count

def create_orders_index(es_url, fake, count=80):
    """Create orders index"""
    
    mapping = {
        "mappings": {
            "properties": {
                "order_date": {"type": "date"},
                "user_id": {"type": "keyword"},
                "status": {"type": "keyword"},
                "total_amount": {"type": "float"},
                "payment_method": {"type": "keyword"}
            }
        }
    }
    
    requests.put(f"{es_url}/orders", json=mapping)
    
    inserted_count = 0
    statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
    payment_methods = ['credit_card', 'paypal', 'bank_transfer']
    
    for i in range(count):
        order = {
            'order_date': fake.date_time_between(start_date='-30d').isoformat(),
            'user_id': f"user_{random.randint(1, 100):04d}",
            'status': random.choice(statuses),
            'total_amount': round(random.uniform(10, 500), 2),
            'payment_method': random.choice(payment_methods)
        }
        
        try:
            response = requests.post(f"{es_url}/orders/_doc", json=order)
            if response.status_code in [200, 201]:
                inserted_count += 1
        except Exception as e:
            if i < 5:
                print(f"❌ Error inserting order {i+1}: {e}")
    
    requests.post(f"{es_url}/orders/_refresh")
    print(f"   ✅ Inserted {inserted_count}/{count} orders")
    return inserted_count

if __name__ == "__main__":
    success = generate_and_insert_data()
    if not success:
        print("\n🔧 Try running the debug script first:")
        print("python debug_elasticsearch_data.py")