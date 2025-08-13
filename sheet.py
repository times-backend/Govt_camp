import pandas as pd
import gspread
from datetime import datetime, date , timedelta
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import os


load_dotenv()
GSHEET_CREDS = os.environ["GSHEET_CREDS"]
def authorize_gspread():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GSHEET_CREDS, scope)
    client = gspread.authorize(creds)
    return client

def fetch_sheet_as_df(sheet, worksheet_name):
    worksheet = sheet.worksheet(worksheet_name)
    all_values = worksheet.get_all_values()

    if not all_values:
        raise ValueError("The sheet is empty.")

    # Get and clean headers
    raw_headers = all_values[0]
    cleaned_headers = []
    seen = set()

    for i, h in enumerate(raw_headers):
        header = h.strip()
        if not header or header in seen:
            header = f"Column_{i+1}"  # e.g., Column_1, Column_2, etc.
        seen.add(header)
        cleaned_headers.append(header)

    # Data rows (excluding header)
    data_rows = all_values[1:]

    # Create DataFrame
    df = pd.DataFrame(data_rows, columns=cleaned_headers)
    return df

def dataframe():
    client = authorize_gspread()
    sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1204ZDFWyWPJo9WXf8aQoRaO8ah3ASUgp9zh5kp2c4og/edit")
    try:
        df = fetch_sheet_as_df(sheet, "DAVP")
    except Exception as e:
        print(f"Failed to fetch and clean the sheet: {e}")
        df = pd.DataFrame()  # Fallback to empty DataFrame
    return df


def upload_to_sheet(df=None, sheet_url="https://docs.google.com/spreadsheets/d/1204ZDFWyWPJo9WXf8aQoRaO8ah3ASUgp9zh5kp2c4og/edit", worksheet_name="Sheet1"):
    # Authorize Google Sheets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Current file directory
    creds = ServiceAccountCredentials.from_json_keyfile_name(GSHEET_CREDS, scope)
    client = gspread.authorize(creds)

    # Open the sheet and worksheet
    sheet = client.open_by_url(sheet_url)
    worksheet = sheet.worksheet(worksheet_name)

    # Clean the DataFrame
    for col in df.select_dtypes(include='number').columns:
        df[col] = df[col].fillna(0)
    df = df.fillna('')

    # Convert DataFrame to list of lists
    data = [df.columns.tolist()] + df.values.tolist()

    # Clear the worksheet first
    worksheet.clear()

    # Upload data
    worksheet.update(data)
    print(f" Uploaded data to sheet tab '{worksheet_name}'")

