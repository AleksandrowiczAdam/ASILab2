import gspread
import pandas as pd
import json
from oauth2client.service_account import ServiceAccountCredentials
import os

# Load Google Sheets credentials from JSON stored in environment variable
def load_credentials():
    return json.loads(os.environ['GOOGLE_CREDENTIALS'])

# Authorize and connect to Google Sheets
def authorize_google_sheets():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_json = load_credentials()
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    
    return client

# Push data from CSV to Google Sheet
def push_data_to_sheet(spreadsheet_name, csv_file):
    # Authorize and get the Google Sheets client
    client = authorize_google_sheets()
    
    # Open the Google Sheet
    sheet = client.open(spreadsheet_name).sheet1  # Assuming you're using the first sheet
    
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file)
    
    print(df)
    
    # Clean the DataFrame
    df.replace([float('inf'), float('-inf'), float('nan')], None, inplace=True)
    df.dropna(inplace=True)
    
    print(df)
    
    # Clear existing data
    sheet.clear()
    
    # Get data as a list of lists for gspread
    data = [df.columns.values.tolist()] + df.values.tolist()
    
    # Update the sheet with the new data
    sheet.insert_rows(data, 1)  # Insert data starting from the first row

if __name__ == "__main__":
    # Replace with your actual Google Sheet name and CSV filename
    GOOGLE_SHEET_NAME = "ASILab02"
    CSV_FILE = "data_student_24732.csv"  # Ensure this matches the CSV filename

    push_data_to_sheet(GOOGLE_SHEET_NAME, CSV_FILE)
