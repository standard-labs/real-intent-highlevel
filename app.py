import streamlit as st
import pandas as pd

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

    emails = [", ".join(map(str, e)) for e in emails]
    phones = [", ".join(map(str, p)) for p in phones]

    df_copy['Email 2'] = emails
    df_copy['Phone 2'] = phones

    df_copy = df_copy.drop(columns=['Email 3', 'Phone 3'], errors='ignore')

    df_copy.rename(columns={'Email 2': 'Additional email addresses', 'Phone 2': 'Additional phone numbers'}, inplace=True)

    return df_copy
    


def main():
    """
    Converts Couchdrop to GoHighLevel format

    Drops extra columns
    """
    st.title('Couchdrop to GoHighLevel CSV Converter')

    st.info("""
    Upload a CSV file. The app will convert your Couchdrop CSV into a format that can be imported into GoHighLevel.
    """)

    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)

        # Check if required columns are in the dataframe
        missing_columns = [col for col in COLUMN_MAPPINGS.keys() if col not in df.columns]
        
        if not missing_columns:

            df_filtered = df[list(COLUMN_MAPPINGS.keys())].rename(columns=COLUMN_MAPPINGS)

            df_compiled = columnComplier(df_filtered)

            df = df_compiled

            df_copy = df.copy()

            df_copy["TAG"] = "Prospect"
            df_copy = df_copy[["TAG"] + [c for c in df_copy.columns if c != "TAG"]]

            df = df_copy

            df["Source"] = HASHTAG_MAPPINGS[df.at[0, "Primary Zip"]]
            # Move hashtag to front
            df = df[["Source"] + [c for c in df.columns if c != "Source"]]

            # Display
            st.write("Converted DataFrame:")
            st.write(df)
                
            # Download
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download converted CSV",
                data=csv,
                file_name='converted_file.csv',
                mime='text/csv',
            )
        else:
            st.write(f"The uploaded file does not contain the required columns: {', '.join(missing_columns)}.")


if __name__ == "__main__":
    main()