from datetime import datetime

def iso_now():
    return datetime.utcnow().isoformat()
