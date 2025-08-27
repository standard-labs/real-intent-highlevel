import requests, time, random
from functools import wraps

class AuthError(Exception):
    """Custom exception for authentication errors."""
    def __init__(self, message):
        super().__init__(message)
        self.message = message

def rate_limited():
    """
    Decorator to handle rate limiting for CRM API calls.
    
    Args:
    """
    def decorator(func: callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for _ in range(10):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 429:  # Too Many Requests
                        retry_after = int(e.response.headers.get('Retry-After', 10))
                        sleep_delay: float = retry_after + (random.randint(50, 100) / 100)
                        print("warn", f"Rate limit hit. Retrying in {sleep_delay} seconds.")
                        time.sleep(sleep_delay)
                    else:
                        raise
            raise Exception(f"Max retries (10) exceeded due to rate limiting.")
        return wrapper
    return decorator
