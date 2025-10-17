import pandas as pd

# Data Extraction
def run_extraction():
    try:
        data = pd.read_csv('zipco_transaction.csv')
        print('Data Extraction Successful')
    except Exception as e:
        print(f"An error occurred: {e}")