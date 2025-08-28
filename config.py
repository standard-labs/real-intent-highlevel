import os
from dotenv import load_dotenv
import streamlit as st

load_dotenv()

# For OAuth2.0 authentication
HIGHLEVEL_AUTH_URL = os.getenv("HIGHLEVEL_AUTH_URL") or st.secrets["HIGHLEVEL_AUTH_URL"]
HIGHLEVEL_URL = os.getenv("HIGHLEVEL_URL") or st.secrets["HIGHLEVEL_URL"]
CLIENT_ID = os.getenv("CLIENT_ID") or st.secrets["CLIENT_ID"]
CLIENT_SECRET = os.getenv("CLIENT_SECRET") or st.secrets["CLIENT_SECRET"]
REDIRECT_URI = os.getenv("REDIRECT_URI") or st.secrets["REDIRECT_URI"]

# For API requests
HIGHLEVEL_API_URL = os.getenv("HIGHLEVEL_API_URL") or st.secrets["HIGHLEVEL_API_URL"]
