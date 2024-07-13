import imaplib
import email
import csv
import re
import io
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def fetch_client_metadata():
    file_path = 'source_files/client_metadata.csv'
    try:
        with open(file_path, 'r', newline='') as csvfile:
            csv_reader = csv.reader(csvfile)
            next(csv_reader) # skip header row
            # Convert each row to a tuple and create a list of these tuples
            data = [row for row in csv_reader]
        return data
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return []
    except csv.Error as e:
        print(f"Error reading CSV file: {e}")
        return []

def extract_client_data(IMAP_connection, client):
    month = (datetime.now().replace(day=1) - timedelta(days=1)).strftime("%B")
    year = (datetime.now().replace(day=1) - timedelta(days=1)).strftime("%Y")
    print(f'\nstarting extract for {client}, {month} {year}')
    IMAP_connection.select("inbox")

    # Search for the email with the specific subject
    date_since = (datetime.now() - timedelta(days=30)).strftime("%d-%b-%Y")
    email_subject = f'{client}: {month} {year} data'
    search_criteria = f'(SUBJECT "{email_subject}" SINCE "{date_since}")'
    print('searching mailbox...')
    _, message_numbers = IMAP_connection.search(None, search_criteria)
    print('finished search')

    if not message_numbers[0]:
        print(f'No email found with the subject "{client}: {month} {year} data"')
        return None

    latest_email_id = message_numbers[0].split()[-1]
    _, msg_data = IMAP_connection.fetch(latest_email_id, "(RFC822)")
    email_body = msg_data[0][1]

    month_dict = {
        'january': '01', 'jan': '01',
        'february': '02', 'feb': '02',
        'march': '03', 'mar': '03',
        'april': '04', 'apr': '04',
        'may': '05',
        'june': '06', 'jun': '06',
        'july': '07', 'jul': '07',
        'august': '08', 'aug': '08',
        'september': '09', 'sep': '09',
        'october': '10', 'oct': '10',
        'november': '11', 'nov': '11',
        'december': '12', 'dec': '12'
    }
    
    email_message = email.message_from_bytes(email_body)
    for part in email_message.walk():
        if part.get_content_maintype() == "multipart":
            continue
        if part.get("Content-Disposition") is None:
            continue
        
        filename = part.get_filename()
        if filename and filename.endswith(".csv"):
            csv_data = part.get_payload(decode=True)
            df = pd.read_csv(io.BytesIO(csv_data))
            df = df.astype(str)
            df['year_month'] = f'{year}-{month_dict[month.lower()]}'
            print("DataFrame created successfully")
            return df
    else:
        raise Exception("No CSV attachment found in the email.")

def base_transform(df, client_metadata):
    df_transformed = df.copy()
    
    # 1. Replace unknown type values with null
    unknown_values = ['N/A', 'unknown', 'NULL', '']
    df_transformed = df_transformed.replace(unknown_values, np.nan)
    
    # 2. Trim whitespace for all string values
    for column in df_transformed.select_dtypes(include=['object']).columns:
        df_transformed[column] = df_transformed[column].astype(str).str.strip()

    # 3. Remove any currency characters and commas from revenue data
    pattern = r"'([^']*)'"
    revenue_fields = re.findall(pattern, client_metadata[4])
    for field in revenue_fields:
        df_transformed[field] = df_transformed[field].replace(r'[€£$,]', '', regex=True).astype(float)
    
    # 4. Standardise data. Each client needs: 
        # client_name
        # year_month
        # product
        # stock
        # sold
        # revenue
    for i in range(4):
        stock_fields = re.findall(pattern, client_metadata[i+1])
        for col in stock_fields:
            if i == 0:
                transformed_col = f"df_transformed['{col}']"
            else:
                transformed_col = f"df_transformed['{col}'].astype(float)"
            client_metadata[i+1] = client_metadata[i+1].replace(f"'{col}'", transformed_col)
    
    std_df = pd.DataFrame({
        'client_name': client_metadata[0],
        'year_month': df_transformed['year_month'],
        'product': eval(client_metadata[1]),
        'stock': eval(client_metadata[2]),
        'sold': eval(client_metadata[3]),
        'revenue': eval(client_metadata[4])
    })

    return std_df


def main():
    # Email account details
    # TODO: implement AWS Secrets Manager for email account access
    EMAIL = "XXX"
    PASSWORD = "YYY"
    IMAP_SERVER = "ZZZ"

    # Connect to the IMAP server
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL, PASSWORD)
    print('successfully logged in')
    
    client_metadata = fetch_client_metadata()

    std_client_list = []
    for client in client_metadata:
        raw_df = extract_client_data(mail, client[0])
        if raw_df is None:
            continue
        std_df = base_transform(raw_df, client)
        std_client_list.append(std_df)

    # Close the IMAP connection
    mail.close()
    mail.logout()
    print('successfully logged out')

    # union std client data
    union_std_client_data = pd.concat(std_client_list, ignore_index=True)

    # Snowflake connection parameters
    # TODO: implement AWS Secrets Manager for Snowflake account access
    conn_params = {
        'user': 'AAA',
        'password': 'BBB',
        'account': 'CCC',
        'warehouse': 'DDD',
        'database': 'EEE',
        'schema': 'FFF'
    }

    # Load data into Snowflake
    print('\nattempting to load data into Snowflake')
    snow_conn = snowflake.connector.connect(**conn_params)
    success, nchunks, nrows, _ = write_pandas(snow_conn, union_std_client_data, 'STD_CLIENT_DATA')
    print(f"Success: {success}, Number of Chunks: {nchunks}, Number of Rows: {nrows}")
    snow_conn.close()

    
    # TODO: implement loading raw data into AWS S3 bucket


if __name__ == "__main__":
    main()