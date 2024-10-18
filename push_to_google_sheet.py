import gspread
import pandas as pd
import json
from oauth2client.service_account import ServiceAccountCredentials
import os
import logging

# Set up logging for the main log file
logging.basicConfig(
    filename='log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Set up a separate logger for the report file
report_logger = logging.getLogger('report_logger')
report_handler = logging.FileHandler('report.txt')
report_handler.setLevel(logging.INFO)
report_formatter = logging.Formatter('%(message)s')  # Only log messages, no timestamp or level
report_handler.setFormatter(report_formatter)
report_logger.addHandler(report_handler)

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
    logging.info("Data cleaning started.")
    
    total_cells = df.size  # Total number of cells in the DataFrame
    total_rows = df.shape[0]  # Total number of rows

    # Initialize counters for modifications and deletions
    total_modified = 0
    total_discarded = 0

    # Clean Age column
    median_age = df['Wiek'].median()
    age_before = df['Wiek'].isnull().sum()
    df['Wiek'].fillna(median_age, inplace=True)
    age_filled = age_before - df['Wiek'].isnull().sum()
    total_modified += age_filled
    logging.info(f"Filled {age_filled} rows with missing Age values with median {median_age}.")

    # Clean Average Salary column
    median_salary = df['Średnie Zarobki'].median()
    salary_before = df['Średnie Zarobki'].isnull().sum()
    df['Średnie Zarobki'].fillna(median_salary, inplace=True)
    salary_filled = salary_before - df['Średnie Zarobki'].isnull().sum()
    total_modified += salary_filled
    logging.info(f"Filled {salary_filled} rows with missing Average Salary values with median {median_salary}.")

    # Discard rows with any remaining missing data
    rows_before = df.shape[0]
    df.dropna(inplace=True)
    rows_after = df.shape[0]
    discarded_rows = rows_before - rows_after
    total_discarded += discarded_rows
    logging.info(f"Discarded {discarded_rows} rows due to missing data.")

    # Normalize Age and Average Salary
    df['Wiek'] = normalize(df['Wiek'])
    df['Średnie Zarobki'] = normalize(df['Średnie Zarobki'])
    total_modified += df['Wiek'].notna().sum() + df['Średnie Zarobki'].notna().sum()

    # Normalize Trip Start and End Time
    def time_to_minutes(time_str):
        if isinstance(time_str, str) and ':' in time_str:
            hours, minutes = map(int, time_str.split(':'))
            return hours * 60 + minutes
        return None

    df['Czas Początkowy Podróży'] = df['Czas Początkowy Podróży'].apply(time_to_minutes)
    df['Czas Końcowy Podróży'] = df['Czas Końcowy Podróży'].apply(time_to_minutes)

    df['Czas Początkowy Podróży'] = normalize(df['Czas Początkowy Podróży'])
    df['Czas Końcowy Podróży'] = normalize(df['Czas Końcowy Podróży'])
    total_modified += df['Czas Początkowy Podróży'].notna().sum() + df['Czas Końcowy Podróży'].notna().sum()

    # Replace Education strings with numeric values
    df['Wykształcenie'] = df['Wykształcenie'].apply(replace_education)
    total_modified += df['Wykształcenie'].notna().sum()
    
    # Calculate total modified and discarded percentages
    modified_percentage = (total_modified / total_cells) * 100 if total_cells > 0 else 0
    discarded_percentage = (total_discarded / total_rows) * 100 if total_rows > 0 else 0

    logging.info(f"Data cleaning completed. Total modified cells: {total_modified}. "
                 f"Total discarded rows: {total_discarded}.")

    # Log the percentages to the report file
    report_logger.info(f"Percentage of modified data: {modified_percentage:.2f}%.")
    report_logger.info(f"Percentage of discarded data: {discarded_percentage:.2f}%.")

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
