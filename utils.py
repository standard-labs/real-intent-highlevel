import requests, time, random
import pandas as pd
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


def columnComplier(df):
    """
    Main logic function to combine multiple email and phone number columns into one

    Input: Pandas Dataframe with columns 'Email 2', 'Email 3', 'Phone 2', and 'Phone 3'
    """
    emails = []
    phones = []
    df_copy = df.copy()

    for _, row in df_copy.iterrows():
        email_list = []
        phone_list = []

        # Combine Email 2 and 3
        if pd.notna(row.get('Email 2')):
            email_list.append(row['Email 2'])
        if pd.notna(row.get('Email 3')):
            email_list.append(row['Email 3'])

        # Combine Phone 2 and 3
        if pd.notna(row.get('Phone 2')):
            phone_list.append(row['Phone 2'])
        if pd.notna(row.get('Phone 3')):
            phone_list.append(row['Phone 3'])

        emails.append(email_list)
        phones.append(phone_list)

    def safe_phone_str(p):
        try:
            # Get rid of float values, as they cause a number with .0
            f = float(p)
            i = int(f)
            if f == i:
                return str(i)
            return str(p)
        except:
            return str(p)

    phones = [[safe_phone_str(p) for p in phone_list] for phone_list in phones]

    # Convert each phone number to have a '+1' US code in front
    for i in range(len(phones)):
        for j in range(len(phones[i])):
            phones[i][j] = "+1" + phones[i][j]

    # Join additional emails/phone numbers
    emails = [", ".join(map(str, e)) for e in emails]
    phones = [", ".join(map(str, p)) for p in phones]

    df_copy['Email 2'] = emails
    df_copy['Phone 2'] = phones

    df_copy = df_copy.drop(columns=['Email 3', 'Phone 3'], errors='ignore')

    df_copy.rename(columns={'Email 2': 'Additional email addresses', 'Phone 2': 'Additional phone numbers'}, inplace=True)

    return df_copy