#!/usr/bin/env python3
"""
Elasticsearch Data Generator using Faker
Generates synthetic data to support the example queries in the LLM Elasticsearch Agent.

This script creates multiple indices with realistic data:
- users: User accounts and profiles
- error_logs: Application error logs
- user_activities: User activity tracking
- system_metrics: System performance data
- orders: E-commerce order data
- products: Product catalog
"""

import os
import sys
import json
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any
import argparse

# Add elasticsearch and faker to requirements
try:
    from elasticsearch import Elasticsearch, helpers
    from faker import Faker
    from faker.providers import internet, company, automotive, credit_card
except ImportError as e:
    print(f"❌ Missing required packages. Please install:")
    print("pip install elasticsearch faker")
    sys.exit(1)

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

class ElasticsearchDataGenerator:
    """Generate synthetic data for Elasticsearch indices."""
    
    def __init__(self, es_host: str = None, es_port: int = 9200):
        """Initialize the data generator."""
        # Use single locale to avoid provider issues
        self.fake = Faker('en_US')
        # Add providers to single locale instance
        self.fake.add_provider(internet)
        self.fake.add_provider(company)
        self.fake.add_provider(automotive)
        self.fake.add_provider(credit_card)
        
        # Elasticsearch connection
        es_host = es_host or os.getenv('ES_HOST', 'localhost')
        if es_host.startswith('http'):
            self.es_url = es_host
        else:
            self.es_url = f"http://{es_host}:{es_port}"
            
        self.es = Elasticsearch([self.es_url])
        
        # Test connection
        try:
            if not self.es.ping():
                raise ConnectionError("Cannot connect to Elasticsearch")
            print(f"✅ Connected to Elasticsearch at {self.es_url}")
        except Exception as e:
            print(f"❌ Failed to connect to Elasticsearch: {e}")
            sys.exit(1)
    
    def delete_existing_indices(self, indices: List[str]):
        """Delete existing indices to start fresh."""
        for index in indices:
            if self.es.indices.exists(index=index):
                self.es.indices.delete(index=index)
                print(f"🗑️  Deleted existing index: {index}")
    
    def create_index_mapping(self, index_name: str, mapping: Dict[str, Any]):
        """Create index with custom mapping."""
        try:
            self.es.indices.create(index=index_name, body=mapping)
            print(f"📋 Created index mapping for: {index_name}")
        except Exception as e:
            print(f"⚠️  Warning: Could not create mapping for {index_name}: {e}")
    
    def generate_users_data(self, count: int = 1000) -> List[Dict[str, Any]]:
        """Generate user account data."""
        print(f"👥 Generating {count} user records...")
        
        users = []
        user_statuses = ['active', 'inactive', 'suspended', 'pending']
        user_roles = ['user', 'admin', 'moderator', 'premium_user', 'guest']
        
        for i in range(count):
            created_date = self.fake.date_time_between(start_date='-2y', end_date='-1m')
            last_login = self.fake.date_time_between(start_date=created_date, end_date='now')
            
            user = {
                'user_id': f"user_{i+1:06d}",
                'username': self.fake.user_name(),
                'email': self.fake.email(),
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'full_name': None,  # Will be set below
                'age': random.randint(18, 75),
                'status': random.choice(user_statuses),
                'role': random.choice(user_roles),
                'created_date': created_date.isoformat(),
                'last_login': last_login.isoformat(),
                'login_count': random.randint(1, 500),
                'country': self.fake.country(),
                'city': self.fake.city(),
                'phone': self.fake.phone_number(),
                'is_verified': self.fake.boolean(chance_of_getting_true=80),
                'subscription_tier': random.choice(['free', 'basic', 'premium', 'enterprise']),
                'account_balance': round(random.uniform(0, 10000), 2),
                'preferences': {
                    'newsletter': self.fake.boolean(),
                    'notifications': self.fake.boolean(),
                    'theme': random.choice(['light', 'dark', 'auto']),
                    'language': random.choice(['en', 'es', 'fr', 'de', 'it'])
                },
                'metadata': {
                    'referral_source': random.choice(['google', 'facebook', 'twitter', 'direct', 'email']),
                    'device_type': random.choice(['mobile', 'desktop', 'tablet']),
                    'browser': random.choice(['chrome', 'firefox', 'safari', 'edge', 'other'])
                }
            }
            
            user['full_name'] = f"{user['first_name']} {user['last_name']}"
            users.append(user)
        
        return users
    
    def generate_error_logs_data(self, count: int = 2000) -> List[Dict[str, Any]]:
        """Generate application error log data."""
        print(f"🚨 Generating {count} error log records...")
        
        error_logs = []
        error_levels = ['ERROR', 'CRITICAL', 'WARNING', 'FATAL']
        error_types = [
            'DatabaseConnectionError', 'AuthenticationError', 'ValidationError',
            'TimeoutError', 'PermissionError', 'FileNotFoundError', 'NetworkError',
            'ConfigurationError', 'OutOfMemoryError', 'NullPointerException',
            'IndexOutOfBoundsError', 'IllegalArgumentError', 'ServiceUnavailableError'
        ]
        services = [
            'user-service', 'payment-service', 'notification-service', 
            'auth-service', 'data-service', 'api-gateway', 'web-app', 
            'mobile-app', 'background-worker', 'scheduler'
        ]
        
        for i in range(count):
            timestamp = self.fake.date_time_between(start_date='-30d', end_date='now')
            error_type = random.choice(error_types)
            service = random.choice(services)
            
            error_log = {
                'log_id': f"log_{i+1:08d}",
                'timestamp': timestamp.isoformat(),
                'level': random.choice(error_levels),
                'error_type': error_type,
                'service': service,
                'message': f"{error_type} occurred in {service}: {self.fake.sentence()}",
                'stack_trace': self._generate_stack_trace(),
                'user_id': f"user_{random.randint(1, 1000):06d}" if random.random() > 0.3 else None,
                'session_id': self.fake.uuid4(),
                'request_id': self.fake.uuid4(),
                'ip_address': self.fake.ipv4(),
                'user_agent': self.fake.user_agent(),
                'url': self.fake.url(),
                'http_method': random.choice(['GET', 'POST', 'PUT', 'DELETE', 'PATCH']),
                'http_status': random.choice([400, 401, 403, 404, 500, 502, 503, 504]),
                'response_time_ms': random.randint(100, 30000),
                'environment': random.choice(['production', 'staging', 'development']),
                'version': f"v{random.randint(1, 5)}.{random.randint(0, 20)}.{random.randint(0, 10)}",
                'resolved': self.fake.boolean(chance_of_getting_true=70),
                'resolution_time': random.randint(5, 1440) if random.random() > 0.3 else None,  # minutes
                'tags': random.sample(['critical', 'urgent', 'bug', 'performance', 'security', 'ui', 'api'], k=random.randint(1, 3))
            }
            
            error_logs.append(error_log)
        
        return error_logs
    
    def generate_user_activities_data(self, count: int = 5000) -> List[Dict[str, Any]]:
        """Generate user activity tracking data."""
        print(f"📊 Generating {count} user activity records...")
        
        activities = []
        activity_types = [
            'login', 'logout', 'page_view', 'click', 'search', 'purchase', 
            'download', 'upload', 'share', 'comment', 'like', 'follow',
            'unfollow', 'update_profile', 'change_password', 'add_to_cart',
            'remove_from_cart', 'checkout', 'payment', 'subscription'
        ]
        
        pages = [
            '/home', '/dashboard', '/profile', '/settings', '/search', '/products',
            '/cart', '/checkout', '/orders', '/help', '/about', '/contact',
            '/login', '/register', '/forgot-password', '/terms', '/privacy'
        ]
        
        # Create activity clusters for more realistic "most active users"
        active_users = [f"user_{i:06d}" for i in range(1, 101)]  # Top 100 active users
        regular_users = [f"user_{i:06d}" for i in range(101, 1001)]  # Regular users
        
        for i in range(count):
            # 60% of activities from active users, 40% from regular users
            if random.random() < 0.6:
                user_id = random.choice(active_users)
                # Active users have more sessions per day
                session_activities = random.randint(3, 15)
            else:
                user_id = random.choice(regular_users)
                session_activities = random.randint(1, 5)
            
            timestamp = self.fake.date_time_between(start_date='-7d', end_date='now')
            activity_type = random.choice(activity_types)
            
            activity = {
                'activity_id': f"activity_{i+1:08d}",
                'user_id': user_id,
                'timestamp': timestamp.isoformat(),
                'activity_type': activity_type,
                'page_url': random.choice(pages) if activity_type == 'page_view' else None,
                'session_id': self.fake.uuid4(),
                'ip_address': self.fake.ipv4(),
                'user_agent': self.fake.user_agent(),
                'device_type': random.choice(['mobile', 'desktop', 'tablet']),
                'browser': random.choice(['chrome', 'firefox', 'safari', 'edge']),
                'location': {
                    'country': self.fake.country(),
                    'city': self.fake.city(),
                    'latitude': float(self.fake.latitude()),
                    'longitude': float(self.fake.longitude())
                },
                'duration_seconds': random.randint(1, 1800) if activity_type == 'page_view' else None,
                'metadata': self._generate_activity_metadata(activity_type),
                'referrer': random.choice(['google.com', 'facebook.com', 'twitter.com', 'direct', None]),
                'conversion_value': round(random.uniform(0, 500), 2) if activity_type in ['purchase', 'subscription'] else None
            }
            
            activities.append(activity)
        
        return activities
    
    def generate_system_metrics_data(self, count: int = 1000) -> List[Dict[str, Any]]:
        """Generate system performance metrics data."""
        print(f"⚡ Generating {count} system metrics records...")
        
        metrics = []
        services = ['web-server', 'database', 'cache', 'queue', 'storage', 'cdn']
        
        for i in range(count):
            timestamp = self.fake.date_time_between(start_date='-7d', end_date='now')
            service = random.choice(services)
            
            metric = {
                'metric_id': f"metric_{i+1:08d}",
                'timestamp': timestamp.isoformat(),
                'service': service,
                'cpu_usage_percent': round(random.uniform(10, 95), 2),
                'memory_usage_percent': round(random.uniform(20, 90), 2),
                'disk_usage_percent': round(random.uniform(30, 85), 2),
                'network_in_mbps': round(random.uniform(1, 1000), 2),
                'network_out_mbps': round(random.uniform(1, 800), 2),
                'response_time_ms': random.randint(50, 5000),
                'requests_per_second': random.randint(10, 10000),
                'error_rate_percent': round(random.uniform(0, 5), 3),
                'availability_percent': round(random.uniform(95, 100), 3),
                'active_connections': random.randint(50, 2000),
                'queue_size': random.randint(0, 1000),
                'cache_hit_rate_percent': round(random.uniform(70, 99), 2)
            }
            
            metrics.append(metric)
        
        return metrics
    
    def generate_orders_data(self, count: int = 800) -> List[Dict[str, Any]]:
        """Generate e-commerce order data."""
        print(f"🛒 Generating {count} order records...")
        
        orders = []
        order_statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded']
        payment_methods = ['credit_card', 'paypal', 'bank_transfer', 'apple_pay', 'google_pay']
        
        for i in range(count):
            order_date = self.fake.date_time_between(start_date='-1y', end_date='now')
            
            order = {
                'order_id': f"order_{i+1:08d}",
                'user_id': f"user_{random.randint(1, 1000):06d}",
                'order_date': order_date.isoformat(),
                'status': random.choice(order_statuses),
                'total_amount': round(random.uniform(10, 2000), 2),
                'currency': 'USD',
                'payment_method': random.choice(payment_methods),
                'shipping_address': {
                    'street': self.fake.street_address(),
                    'city': self.fake.city(),
                    'state': self.fake.state(),
                    'country': self.fake.country(),
                    'postal_code': self.fake.postcode()
                },
                'items': [
                    {
                        'product_id': f"product_{random.randint(1, 100):04d}",
                        'name': self.fake.catch_phrase(),
                        'quantity': random.randint(1, 5),
                        'price': round(random.uniform(5, 500), 2)
                    } for _ in range(random.randint(1, 4))
                ],
                'discount_amount': round(random.uniform(0, 100), 2) if random.random() > 0.7 else 0,
                'tax_amount': 0,  # Will be calculated
                'shipping_cost': round(random.uniform(0, 50), 2),
                'notes': self.fake.text(max_nb_chars=100) if random.random() > 0.8 else None
            }
            
            # Calculate tax (8% of subtotal)
            subtotal = sum(item['price'] * item['quantity'] for item in order['items'])
            order['tax_amount'] = round(subtotal * 0.08, 2)
            
            orders.append(order)
        
        return orders
    
    def _generate_stack_trace(self) -> str:
        """Generate a realistic stack trace."""
        traces = [
            "at com.example.service.UserService.getUserById(UserService.java:45)",
            "at com.example.controller.UserController.getUser(UserController.java:23)",
            "at java.base/java.lang.Thread.run(Thread.java:829)"
        ]
        return "\n".join(traces)
    
    def _generate_activity_metadata(self, activity_type: str) -> Dict[str, Any]:
        """Generate metadata specific to activity type."""
        if activity_type == 'search':
            return {'query': self.fake.sentence(nb_words=3), 'results_count': random.randint(0, 1000)}
        elif activity_type == 'purchase':
            return {'product_id': f"product_{random.randint(1, 100):04d}", 'amount': round(random.uniform(10, 500), 2)}
        elif activity_type == 'click':
            return {'element': random.choice(['button', 'link', 'image', 'menu_item'])}
        else:
            return {}
    
    def bulk_index_data(self, index_name: str, data: List[Dict[str, Any]], doc_type: str = '_doc'):
        """Bulk index data to Elasticsearch."""
        print(f"📤 Indexing {len(data)} documents to '{index_name}'...")
        
        def doc_generator():
            for doc in data:
                yield {
                    "_index": index_name,
                    "_type": doc_type,
                    "_source": doc
                }
        
        try:
            success, failed = helpers.bulk(
                self.es,
                doc_generator(),
                chunk_size=500,
                request_timeout=60
            )
            print(f"✅ Successfully indexed {success} documents to '{index_name}'")
            if failed:
                print(f"⚠️  Failed to index {len(failed)} documents")
        except Exception as e:
            print(f"❌ Error indexing to '{index_name}': {e}")
    
    def create_index_mappings(self):
        """Create index mappings for better search performance."""
        
        # Users index mapping
        users_mapping = {
            "mappings": {
                "properties": {
                    "user_id": {"type": "keyword"},
                    "username": {"type": "keyword"},
                    "email": {"type": "keyword"},
                    "full_name": {"type": "text", "analyzer": "standard"},
                    "created_date": {"type": "date"},
                    "last_login": {"type": "date"},
                    "login_count": {"type": "integer"},
                    "status": {"type": "keyword"},
                    "role": {"type": "keyword"},
                    "age": {"type": "integer"},
                    "country": {"type": "keyword"},
                    "city": {"type": "keyword"}
                }
            }
        }
        
        # Error logs index mapping
        error_logs_mapping = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "level": {"type": "keyword"},
                    "error_type": {"type": "keyword"},
                    "service": {"type": "keyword"},
                    "message": {"type": "text", "analyzer": "standard"},
                    "user_id": {"type": "keyword"},
                    "ip_address": {"type": "ip"},
                    "http_status": {"type": "integer"},
                    "response_time_ms": {"type": "integer"},
                    "environment": {"type": "keyword"},
                    "resolved": {"type": "boolean"},
                    "tags": {"type": "keyword"}
                }
            }
        }
        
        # User activities index mapping
        activities_mapping = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "user_id": {"type": "keyword"},
                    "activity_type": {"type": "keyword"},
                    "session_id": {"type": "keyword"},
                    "device_type": {"type": "keyword"},
                    "browser": {"type": "keyword"},
                    "ip_address": {"type": "ip"},
                    "page_url": {"type": "keyword"},
                    "conversion_value": {"type": "float"},
                    "location.country": {"type": "keyword"},
                    "location.city": {"type": "keyword"}
                }
            }
        }
        
        # System metrics index mapping
        metrics_mapping = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "service": {"type": "keyword"},
                    "cpu_usage_percent": {"type": "float"},
                    "memory_usage_percent": {"type": "float"},
                    "response_time_ms": {"type": "integer"},
                    "requests_per_second": {"type": "integer"},
                    "error_rate_percent": {"type": "float"},
                    "availability_percent": {"type": "float"}
                }
            }
        }
        
        # Orders index mapping
        orders_mapping = {
            "mappings": {
                "properties": {
                    "order_date": {"type": "date"},
                    "user_id": {"type": "keyword"},
                    "status": {"type": "keyword"},
                    "total_amount": {"type": "float"},
                    "payment_method": {"type": "keyword"},
                    "currency": {"type": "keyword"}
                }
            }
        }
        
        mappings = {
            'users': users_mapping,
            'error_logs': error_logs_mapping,
            'user_activities': activities_mapping,
            'system_metrics': metrics_mapping,
            'orders': orders_mapping
        }
        
        for index_name, mapping in mappings.items():
            self.create_index_mapping(index_name, mapping)
    
    def generate_all_data(self, 
                         users_count: int = 1000,
                         error_logs_count: int = 2000,
                         activities_count: int = 5000,
                         metrics_count: int = 1000,
                         orders_count: int = 800,
                         clean_existing: bool = True):
        """Generate all synthetic data and index to Elasticsearch."""
        
        print("🚀 Starting Elasticsearch data generation...")
        print(f"Target: {self.es_url}")
        print("-" * 50)
        
        indices = ['users', 'error_logs', 'user_activities', 'system_metrics', 'orders']
        
        if clean_existing:
            self.delete_existing_indices(indices)
        
        # Create index mappings
        self.create_index_mappings()
        
        # Generate and index data
        datasets = [
            ('users', self.generate_users_data(users_count)),
            ('error_logs', self.generate_error_logs_data(error_logs_count)),
            ('user_activities', self.generate_user_activities_data(activities_count)),
            ('system_metrics', self.generate_system_metrics_data(metrics_count)),
            ('orders', self.generate_orders_data(orders_count))
        ]
        
        for index_name, data in datasets:
            self.bulk_index_data(index_name, data)
        
        print("-" * 50)
        print("🎉 Data generation completed successfully!")
        print("\n📊 Summary:")
        print(f"• Users: {users_count}")
        print(f"• Error Logs: {error_logs_count}")
        print(f"• User Activities: {activities_count}")
        print(f"• System Metrics: {metrics_count}")
        print(f"• Orders: {orders_count}")
        
        print(f"\n💡 You can now query your data using the LLM Elasticsearch Agent!")
        print(f"   Example queries:")
        print(f"   • How many users are in the system?")
        print(f"   • Show me recent error logs")
        print(f"   • What are the top 10 most active users?")
        print(f"   • Find all records from last week")


def main():
    """Main function with command line argument parsing."""
    parser = argparse.ArgumentParser(description='Generate synthetic data for Elasticsearch')
    parser.add_argument('--host', default=None, help='Elasticsearch host (default: from ES_HOST env or localhost)')
    parser.add_argument('--port', type=int, default=9200, help='Elasticsearch port (default: 9200)')
    parser.add_argument('--users', type=int, default=1000, help='Number of users to generate (default: 1000)')
    parser.add_argument('--errors', type=int, default=2000, help='Number of error logs (default: 2000)')
    parser.add_argument('--activities', type=int, default=5000, help='Number of user activities (default: 5000)')
    parser.add_argument('--metrics', type=int, default=1000, help='Number of system metrics (default: 1000)')
    parser.add_argument('--orders', type=int, default=800, help='Number of orders (default: 800)')
    parser.add_argument('--keep-existing', action='store_true', help='Keep existing indices (default: delete and recreate)')
    
    args = parser.parse_args()
    
    try:
        generator = ElasticsearchDataGenerator(es_host=args.host, es_port=args.port)
        generator.generate_all_data(
            users_count=args.users,
            error_logs_count=args.errors,
            activities_count=args.activities,
            metrics_count=args.metrics,
            orders_count=args.orders,
            clean_existing=not args.keep_existing
        )
    except KeyboardInterrupt:
        print("\n⏹️  Data generation interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()