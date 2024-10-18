import gspread
import pandas as pd
import json
from oauth2client.service_account import ServiceAccountCredentials
import os

def normalize(series):
    return (series - series.min()) / (series.max() - series.min())

def replace_education(education):
    mapping = {
        'Podstawowe': 0,
        'Średnie': 1,
        'Wyższe': 2
    }
    return mapping.get(education, -1)  # Default to -1 if unknown

def clean_data(df):
    # Clean Age column
    median_age = df['Wiek'].median()
    df['Wiek'].fillna(median_age, inplace=True)

    # Clean Average Salary column
    median_salary = df['Średnie Zarobki'].median()
    df['Średnie Zarobki'].fillna(median_salary, inplace=True)

    # Discard rows with any remaining missing data
    df.dropna(inplace=True)

    # Normalize Age and Average Salary
    df['Wiek'] = normalize(df['Wiek'])
    df['Średnie Zarobki'] = normalize(df['Średnie Zarobki'])

    # Normalize Trip Start and End Time
    # Convert time strings to minutes for normalization
    def time_to_minutes(time_str):
        if isinstance(time_str, str) and ':' in time_str:
            hours, minutes = map(int, time_str.split(':'))
            return hours * 60 + minutes
        return None

    df['Czas Początkowy Podróży'] = df['Czas Początkowy Podróży'].apply(time_to_minutes)
    df['Czas Końcowy Podróży'] = df['Czas Końcowy Podróży'].apply(time_to_minutes)

    df['Czas Początkowy Podróży'] = normalize(df['Czas Początkowy Podróży'])
    df['Czas Końcowy Podróży'] = normalize(df['Czas Końcowy Podróży'])

    # Replace Education strings with numeric values
    df['Wykształcenie'] = df['Wykształcenie'].apply(replace_education)

    return df


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
   
    clean_data(df)
    
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
