from faker import Faker
from uuid import uuid4
from datetime import datetime, timedelta
import random

fake = Faker()

# Entity definitions with field generators
ENTITY_FIELDS = {
    "Invoice": {
        "amount": lambda: round(random.uniform(100, 1000), 2),
        "status": lambda: random.choice(["Paid", "Unpaid", "Overdue"])
    },
    "Contract": {
        "start_date": lambda: fake.date(),
        "end_date": lambda: fake.date(),
        "terms": lambda: fake.sentence()
    },
    "UserAccount": {
        "email": lambda: fake.email(),
        "role": lambda: random.choice(["Admin", "Viewer", "Editor"]),
        "is_active": lambda: random.choice([True, False])
    },
    "Project": {
        "title": lambda: fake.bs(),
        "deadline": lambda: fake.future_date(),
        "budget": lambda: random.randint(10000, 100000)
    }
}

ROLES = ["Admin", "User", "Manager"]
ACTIONS = ["login", "logout", "update"]

# Configuration
USER_POOL_SIZE = 20
ENTITY_POOL_SIZE_PER_TYPE = 30

# Global pools
_user_pool = []
_entity_pool = {entity: [] for entity in ENTITY_FIELDS.keys()}


def create_user_pool():
    global _user_pool
    _user_pool = [
        {
            "id": str(uuid4()),
            "name": fake.name(),
            "role": random.choice(ROLES)
        }
        for _ in range(USER_POOL_SIZE)
    ]


def create_entity_pool():
    global _entity_pool
    for entity_type in ENTITY_FIELDS:
        _entity_pool[entity_type] = [str(uuid4()) for _ in range(ENTITY_POOL_SIZE_PER_TYPE)]


def get_random_user():
    return random.choice(_user_pool)


def get_random_entity(entity_type):
    return random.choice(_entity_pool[entity_type])


def generate_entity_change(entity_type):
    fields = ENTITY_FIELDS[entity_type]
    changed_fields = random.sample(list(fields.keys()), k=random.randint(1, len(fields)))
    changes = []
    for f in changed_fields:
        old = str(fields[f]())
        new = str(fields[f]())
        while new == old:
            new = str(fields[f]())
        changes.append({"field": f, "old": old, "new": new})
    return changes


def generate_audit_log():
    entity_type = random.choice(list(ENTITY_FIELDS.keys()))
    user = get_random_user()
    entity_id = get_random_entity(entity_type)
    login_time = datetime.utcnow() - timedelta(minutes=random.randint(30, 90))
    logout_time = login_time + timedelta(minutes=random.randint(5, 30))

    return {
        "log_id": str(uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "user": user,
        "session": {
            "login_time": login_time.isoformat(),
            "logout_time": logout_time.isoformat()
        },
        "action": "update",
        "entity": {
            "type": entity_type,
            "id": entity_id,
            "field_changes": generate_entity_change(entity_type)
        },
        "client_ip": fake.ipv4()
    }


def generate_bulk_logs(n=1000):
    create_user_pool()
    create_entity_pool()
    return [generate_audit_log() for _ in range(n)]
