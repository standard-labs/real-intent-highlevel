import requests
import urllib.parse
import random
import string
import streamlit as st

from config import CLIENT_ID, CLIENT_SECRET, HIGHLEVEL_AUTH_URL, REDIRECT_URI, HIGHLEVEL_API_URL
from utils import AuthError

def reset_session():
    st.session_state["authenticated"] = False
    st.session_state["access_token"] = None
    st.session_state["refresh_token"] = None
    st.session_state["username"] = None
    st.session_state["state"] = None
    st.session_state["location_id"] = None


def generate_state():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=16))


def get_auth_url():
    state = generate_state()
    st.session_state["state"] = state

    params = {
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "scope": "contacts.write contacts.readonly",
        "state": state,
    }

    return f"{HIGHLEVEL_AUTH_URL}?{urllib.parse.urlencode(params)}"


def exchange_code_for_token(code):
    """Exchange and store access and refresh tokens."""
    data = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'redirect_uri': REDIRECT_URI,
        'grant_type': 'authorization_code',
        'code': code,
    }
        

    response = requests.post(f"{HIGHLEVEL_API_URL}/oauth/token", data=data)
    response.raise_for_status()
    
    access_token = response.json().get("access_token", None)
    refresh_token = response.json().get("refresh_token", None)
    location_id = response.json().get("location_id", None)
    
    if not access_token or not refresh_token:
        reset_session()
        raise AuthError("Access or Refresh token not found in response.")

    st.session_state["access_token"] = access_token
    st.session_state["refresh_token"] = refresh_token
    st.session_state["location_id"] = location_id


def refresh_token() -> str:
    
    if "refresh_token" not in st.session_state:
        reset_session()
        raise AuthError("Failed to refresh; No refresh token found.")

    
    data = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'refresh_token',
        'refresh_token': st.session_state["refresh_token"],
    }

    response = requests.post(f"{HIGHLEVEL_API_URL}/oauth/token", data=data)
        
    if not response.ok:
        reset_session()
        raise AuthError("Failed to refresh token.")
    
    new_access_token = response.json().get("access_token", None)
    new_refresh_token = response.json().get("refresh_token", None)
    
    if not new_access_token:
        reset_session()
        raise AuthError("Access token not found in refresh response.")
    
    st.session_state["access_token"] = new_access_token
    st.session_state["refresh_token"] = new_refresh_token if new_refresh_token else st.session_state["refresh_token"] # update refresh only if it is returned
    
    return new_access_token
    

def authenticate(code, state):
    try:
        exchange_code_for_token(code)

        st.session_state["authenticated"] = True
    
    except AuthError as e:
        raise e
    except Exception as e:
        reset_session()
        raise Exception(f"Unexpected authentication error: {e}")

