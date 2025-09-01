import streamlit as st
import pandas as pd

from auth import authenticate, get_auth_url, reset_session
from api import HighLevelDeliverer
from utils import AuthError, columnComplier


# Define global variables for column mappings
COLUMN_MAPPINGS = {
    "first_name": "First Name",
    "last_name": "Last Name",
    "email_1": "Email",
    "email_2": "Email 2",
    "email_3": "Email 3",
    "phone_1": "Phone",
    "phone_2": "Phone 2",
    "phone_3": "Phone 3",
    "address": "Primary Address",
    "city": "Primary City",
    "state": "Primary State",
    "zip_code": "Primary Zip",
}

HASHTAG_MAPPINGS = {
    60177: "SouthElginRealIntent",
    60126: "ElmhurstRealIntent",
    60622: "WestParkWestTownRealIntent",
    60010: "BarringtonRealIntent",
    60045: "LakeForestRealIntent",
    60564: "NapervilleRealIntent"
}

def convertHighLevel(df):
    # Filter datafram column names to match GoHighLevel requirements
    df_filtered = df[list(COLUMN_MAPPINGS.keys())].rename(columns=COLUMN_MAPPINGS)

    # Compile extra emails and phone numbers into one column
    df_compiled = columnComplier(df_filtered)

    df = df_compiled

    df_copy = df.copy()

    # Add a tag to each lead as 'Prospect'
    df_copy["TAG"] = "Prospect"
    df_copy = df_copy[["TAG"] + [c for c in df_copy.columns if c != "TAG"]]

    df = df_copy

    df["Source"] = HASHTAG_MAPPINGS[df.at[0, "Primary Zip"]]
    # Move hashtag to front
    df = df[["Source"] + [c for c in df.columns if c != "Source"]]


    # Convert each main phone number to have a '+1' US code in front
    df["Phone"] = df["Phone"].apply(
        lambda x: "+1" + str(int(x)) if pd.notna(x) else ""
    )
    df["Phone"] = df["Phone"].astype(str)

    return df


def main():
    """
    Converts Couchdrop to GoHighLevel format

    Drops extra columns
    """
    st.title('Couchdrop to GoHighLevel CSV Converter')

    st.info("""
    Upload a CSV file. The app will convert your Couchdrop CSV into a format that can be imported into GoHighLevel.
    """)


    # -- Authentication --
    
    if "code" in st.query_params and "state" in st.query_params: 
        try:
            authenticate(st.query_params["code"], st.query_params["state"])      
            st.query_params.clear()
        except AuthError as e:
            st.warning(f"Authentication Error: {e.message}") 
            st.query_params.clear()   
        except Exception as e:
            st.error(f"Unexpected Error: {e}")
            st.query_params.clear()   
            
    if not st.session_state.get("authenticated"):
        st.markdown(f"[Authenticate with GoHighLevel]({get_auth_url()})")
    else:
        st.success("You are authenticated with GoHighLevel.")


    # Take input
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)

        # Check if required columns are in the dataframe
        missing_columns = [col for col in COLUMN_MAPPINGS.keys() if col not in df.columns]
        
        if not missing_columns:
            df_highlevel = df.copy()

            df = convertHighLevel(df)

            # Display
            st.write("Converted DataFrame:")
            st.write(df)
                
            # Allow the user to either download the CSV or send it directly to GoHighLevel
            option = st.radio("Choose an action", ["Download CSV", "Send to GoHighLevel"])
            # -- Download CSV --

            if option == "Download CSV":
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download converted CSV",
                    data=csv,
                    file_name='converted_file.csv',
                    mime='text/csv',
                )
                
            # -- Send to GoHighLevel --
            
            elif option == "Send to GoHighLevel" and st.session_state.get("authenticated"):
                try:                    
                    if st.button("Deliver Data to GoHighLevel"):
                    
                        with st.spinner("Preparing leads for delivery..."):
                            deliverer = HighLevelDeliverer(
                                access_token=st.session_state["access_token"],
                                location_id=st.session_state["location_id"],
                                n_threads=5
                            )
                        
                            deliver_df = df_highlevel.replace({float('nan'): None}, inplace=False)
                        
                        with st.spinner("Delivering leads..."):
                            deliverer.deliver(deliver_df)
                            failed_leads = deliverer.get_failed_leads()                     

                            if failed_leads:
                                st.error(f"{len(failed_leads)} leads failed to deliver.")
                                
                                with st.expander("Click to see failed lead details"):
                                    for failed in failed_leads:
                                        st.error(f"{failed['md5']}: {failed['error']}")  
                            else:
                                st.success("All leads delivered successfully!")
                                
                except AuthError as e:
                    reset_session()
                    st.warning(f"{e}")
                except Exception as e:
                    st.error(e)
                    
            elif option == "Send to GoHighLevel":
                st.warning("Please authenticate first to send data to GoHighLevel.")
                
        else:
            st.write(f"The uploaded file does not contain the required columns: {', '.join(missing_columns)}.")


if __name__ == "__main__":
    main()