from concurrent.futures import ThreadPoolExecutor
import requests
import datetime
import pandas as pd
from typing import Any

from config import HIGHLEVEL_API_URL
from utils import rate_limited, AuthError
from auth import refresh_token

class HighLevelDeliverer():
    """Delivers data to GoHighLevel CRM."""

    def __init__(
            self, 
            access_token: str, 
            base_url: str = HIGHLEVEL_API_URL,
            n_threads: int = 1,
        ):
        """
        Initialize the HighLevelDeliverer.

        Args:
            access_token (str): The user's access token for GoHighLevel.
            base_url (str, optional): The base URL for the GoHighLevel API. Defaults to HIGHLEVEL_API_URL.
            n_threads (int, optional): The number of threads to use for delivering leads. Defaults to 1.
        """
        
        self.access_token: str = access_token
        self.base_url: str = base_url
        
        # Keep track of failed leads
        self.failed_leads: list[dict] = []

        # Configuration stuff
        self.n_threads: int = n_threads

        # Make sure API credentials are valid
        if not self._verify_api_credentials():
            raise AuthError("Could not verify credentials for GoHighLevel delivery. Please re-authenticate.")
    
    def get_failed_leads(self) -> list[dict]:
        """
        Get the list of failed leads.

        Returns:
            list[dict]: A list of dictionaries containing information about the failed leads.
        """
        return self.failed_leads    
    
    @property
    def api_headers(self) -> dict:
        """
        Generate the API headers for GoHighLevel requests.

        Returns:
            dict: A dictionary containing the necessary headers for API requests.
        """
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Version": "2021-07-28",
            "Content-Type": "application/json",
        }
    
    @rate_limited()
    def _verify_api_credentials(self) -> bool:
        """
        Verify that the API credentials are valid. Refresh the token if necessary.

        Returns:
            bool: True if the credentials are valid, False otherwise.
        """
    
        response = requests.post(
            f"{self.base_url}/contacts/search",
            headers=self.api_headers
        )
        
        if response.status_code == 401:
            self.access_token = refresh_token()
            response = requests.post(
                f"{self.base_url}/contacts/search",
                headers=self.api_headers
            )
            raise AuthError("401 error. The access token did not work")
            return response.ok
        return response.ok
    
    def deliver(self, data: pd.DataFrame) -> list[dict]:
        """
        Deliver the PII data to GoHighLevel.

        Args:
            pii_md5s (list[MD5WithPII]): A list of MD5WithPII objects containing the PII data to be delivered.

        Returns:
            list[dict]: A list of response dictionaries from the GoHighLevel API for each delivered event.
        """
        
        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            return list(executor.map(self._deliver_single_lead, ((row) for _, row in data.iterrows())))

    def _deliver_single_lead(self, lead: pd.Series) -> dict:
        """
        Deliver a single lead to GoHighLevel.

        Args:
            lead (pd.Series): A single row of the dataframe containing the PII data.

        Returns:
            dict: A response dictionary from the GoHighLevel API for the delivered event.
        """
        try:

            event_data = self._prepare_event_data(lead)   
            response = self._send_event(event_data)
            print(
                "trace", 
                (
                    f"Delivered lead: {lead.get('md5')}, "
                    f"response_status: {response.get('status', 'unknown')}"
                )
            )
            return response
        except Exception as e:
            self.failed_leads.append({
                "md5": lead.get("md5"),
                "error": str(e),
            })
            return {
                "status": "failed",
                "error": str(e),
            }

    def _prepare_event_data(self, lead: pd.Series) -> dict:
        """
        Prepare the event data for a single row of the dataframe.

        Args:
            lead (pd.Series): A single row of the dataframe containing the PII data.

        Returns:
            dict: A dictionary containing the prepared event data for the GoHighLevel API.
        """
                
        # get all the required info
        md5: str | None = lead.get("md5")
        firstName: str | None = lead.get("firstName")
        lastName: str | None = lead.get("lastName")
        email: str | None = lead.get("email_1")
        phone_1: str | None = str(lead.get("phone_1")) if lead.get("phone_1") else None
        address: str | None = lead.get("address")
        city: str | None = lead.get("city")
        state: str | None = lead.get("state")
        postalCode: str | None = str(lead.get("postalCode")) if lead.get("postalCode") else None
        
        print("trace", f"Preparing event data for MD5: {md5}, firstName: {firstName}, lastName: {lastName}")

        # Prepare contact info
        contact_info: dict[str, Any] = {}
        
        contact_info["firstName"] = firstName
        contact_info["lastName"] = lastName

        if email:        
            contact_info["email"] = email
        
        def _clean_phone(value):
            try:
                return str(int(float(value)))
            except (ValueError, TypeError):
                return str(value).strip()
            
        # phones won't show up in GoHighLevel, if they aren't exactly 10 digits with no decimal point    
        if phone_1:
            phone_1 = _clean_phone(phone_1)

        contact_info["phone"] = phone_1
        contact_info["address1"] = address
        contact_info["city"] = city
        contact_info["state"] = state
        contact_info["postalCode"] = postalCode
        
        # Add Notes
        notes: list[dict[str, str]] = []
        
        note_field_map = {
            "insight": "AI-Enhanced Insight",
            "phone_1_dnc": "Cell Phone DNC Status",
            "phone_2_dnc": "Home Phone DNC Status",
            "phone_3_dnc": "Work Phone DNC Status",
            "email_2": "Secondary Email",
            "email_3": "Alternative Email",
            "age": "Age",
            "gender": "Gender",
            "head_of_household": "Head of Household",
            "birth_month_and_year": "Birth Month and Year",
            "credit_range": "Credit Range",
            "household_income": "Household Income",
            "household_net_worth": "Household Net Worth",
            "home_owner_status": "Home Owner Status",
            "median_home_value": "Median Home Value",
            "occupation": "Occupation",
            "education": "Education Level",
            "marital_status": "Marital Status",
            "n_household_children": "Number of Children",
            "n_household_adults": "Number of Adults",
            "investments": "Investments",
            "investment_type": "Investment Type",  
        }
        
        note_lines: dict[str, Any] = {}
        for key, label in note_field_map.items():
            value = lead.get(key)
            if pd.notna(value) and value != "":
                note_lines[key] = value

        if note_lines:
            notes.append({
                "content": "\n".join(note_lines),
                "category": "info",
                "created_by": "Real Intent",
                "created_date": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "is_pinned": True,
            })  
                 
            
        # Prepare event data according to GoHighLevel API schema
        event_data: dict[str, Any] = {         
            "firstName": contact_info["firstName"],
            "lastName": contact_info["lastName"],
            "name": contact_info["firstName"] + " " + contact_info["lastName"],
            "email": contact_info["email"],
            # TODO: Update locationId
            "locationId": "ve9EPM428h8vShlRW1KT",
            "gender": note_lines["gender"],
            "phone": contact_info["phone"],
            "address1": contact_info["address1"],
            "city": contact_info["city"],
            "state": contact_info["state"],
            "postalCode": contact_info["postalCode"],
        }
                        
        return event_data

    @rate_limited()
    def _send_event(self, event_data: dict) -> dict:
        """
        Send an event to the GoHighLevel API.

        Args:
            event_data (dict): The prepared event data to be sent to the API.

        Returns:
            dict: The response from the GoHighLevel API, either the JSON response or an ignored status message.

        Raises:
            requests.exceptions.HTTPError: If the API request fails.
        """
        print(
            "trace", 
            (
                f"Sending event to GoHighLevel API, "
                f"person: {event_data}"
            )
        )

        response = requests.post(
            f"{self.base_url}/contacts/upsert", 
            json=event_data, 
            headers=self.api_headers
        )
        
        print("trace", f"Raw response: {response.text}, status_code: {response.status_code}")
                
        response.raise_for_status()
        return response.json()
    