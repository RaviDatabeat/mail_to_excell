import os
import imaplib
import email
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from loguru import logger
from pathlib import Path
import sys
from email.header import decode_header
import numpy as np

load_dotenv() 

class ExcelGSheetAppender:
    def __init__(self):
        self.gmail_user = os.getenv("GMAIL_USER")
        self.gmail_pass = os.getenv("GMAIL_APP_PASSWORD")
        self.sender_email = os.getenv("SENDER_EMAIL")
        self.sheet_url = os.getenv("GOOGLE_SHEET_URL")
        self.worksheet_name = os.getenv("WORKSHEET_NAME")
        self.service_account_file = os.getenv("SERVICE_ACCOUNT_FILE")
        self.run_immediately = os.getenv("RUN_IMMEDIATELY", "False").lower() == "true"
        self.run_hour = int(os.getenv("RUN_HOUR", 22))
        self.run_minute = int(os.getenv("RUN_MINUTE", 0))
        self.appendsheet_name = os.getenv("APPENDWORKSHEET_NAME")
    
    def setup_logging(self):
        logger.remove()
        logs_directory = Path("logs")
        logs_directory.mkdir(exist_ok=True)
        log_file_path = logs_directory / "pipeline_execution.log"
        logger.add(
            log_file_path,
            retention="10 days",
            level="INFO",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
            backtrace=True,
            diagnose=True,
            enqueue=True,
        )
        logger.add(sys.stdout, level="INFO")
    
    def connect_gmail(self):
        try:
            mail = imaplib.IMAP4_SSL('imap.gmail.com')
            mail.login(self.gmail_user, self.gmail_pass)
            mail.select('inbox')
            logger.info("Connect to mail")
            return mail
        except Exception as e:
            logger.critical(f"Error in connection with mail as {e}")
    
    def get_latest_excel(self, mail):
        try:
            status, messages = mail.search(None, f'(FROM "{self.sender_email}")')
            messages = messages[0].split()
            if not messages:
                logger.info("No emails found from the sender.")
                return None

            latest_email_id = messages[-1]
            status, msg_data = mail.fetch(latest_email_id, '(RFC822)')
            msg = email.message_from_bytes(msg_data[0][1])


            tmp_dir = Path.cwd() / "tmp"
            tmp_dir.mkdir(exist_ok=True)

            for part in msg.walk():
                filename = part.get_filename()
                if filename:
                    decoded_name = decode_header(filename)[0][0]
                    if isinstance(decoded_name, bytes):
                        decoded_name = decoded_name.decode()
                    filename = decoded_name

                    if filename.lower().endswith('.csv') or filename.lower().endswith('.xlsx'):
                        file_path = tmp_dir / filename
                        with open(file_path, 'wb') as f:
                            f.write(part.get_payload(decode=True))
                        logger.info(f"Got latest file: {filename}")
                        return str(file_path)

            logger.info("No Excel or CSV attachment found in the latest email.")
            return None

        except Exception as e:
            logger.critical(f"Error fetching latest file: {e}")
            raise ValueError(f"Error fetching latest file: {e}")
    def append_to_mapping_sheet(self, csv_path):
        try:

            df_csv = pd.read_csv(csv_path)
            df_csv.columns = df_csv.columns.str.strip().str.lower()
            df_csv.rename(columns={
                'publication id': 'publication_id',
                'bundle id': 'bundle_id',
                'publication url': 'domain'
            }, inplace=True)


            df_csv['publication_id'] = df_csv['publication_id'].astype(str).str.strip()
            df_csv['bundle_id'] = df_csv['bundle_id'].astype(str).str.strip()
            df_csv['domain'] = df_csv['domain'].astype(str).str.strip()

            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_file(self.service_account_file, scopes=scope)
            gc = gspread.authorize(creds)
            sheet = gc.open_by_url(self.sheet_url)

            no_domain_ws = sheet.worksheet(self.worksheet_name)
            df_no_domain = pd.DataFrame(no_domain_ws.get_all_records())
            df_no_domain.columns = df_no_domain.columns.str.strip().str.lower()

            df_no_domain['publication_id'] = df_no_domain['publication_id'].astype(str).str.strip()
            df_no_domain['bundle_id'] = df_no_domain['bundle_id'].astype(str).str.strip()
            df_no_domain['domain'] = df_no_domain['domain'].astype(str).str.strip()

            df_no_domain = df_no_domain[~df_no_domain['publication_id'].duplicated(keep=False)]
            df_no_domain = df_no_domain.drop_duplicates()

            no_domain_ws.clear()
            no_domain_ws.append_row(df_no_domain.columns.tolist())
            no_domain_ws.append_rows(df_no_domain.values.tolist())

        
            df_merged = df_no_domain.merge(
                df_csv[['publication_id', 'bundle_id', 'domain']],
                on='publication_id',
                how='left',
                suffixes=('_sheet', '_csv')
            )

            df_merged['bundle_id'] = df_merged['bundle_id_csv'].combine_first(df_merged['bundle_id_sheet'])
            df_merged['domain'] = df_merged['domain_csv'].combine_first(df_merged['domain_sheet'])

            merge_cols = ['bundle_id_csv', 'bundle_id_sheet', 'domain_csv', 'domain_sheet']

            

            df_merged = df_merged.dropna(subset=merge_cols, how='all')
            df_final = df_merged.drop(columns=merge_cols)
            df_final = df_final.drop_duplicates()




            # # Create dicts for lookup
            # bundle_map = df_csv.set_index('publication_id')['bundle_id'].to_dict()
            # domain_map = df_csv.set_index('publication_id')['domain'].to_dict()

            # # Fill values in No_Domain|Bundle
            # df_no_domain['bundle_id'] = df_no_domain['publication_id'].map(bundle_map).combine_first(df_no_domain['bundle_id'])
            # df_no_domain['domain'] = df_no_domain['publication_id'].map(domain_map).combine_first(df_no_domain['domain'])

            # df_final = df_no_domain.copy()

        
            mapping_ws = sheet.worksheet(self.appendsheet_name)
            mapping_columns = mapping_ws.row_values(1)
            mapping_columns_lower = [c.strip().lower() for c in mapping_columns]

            for col in mapping_columns_lower:
                if col not in df_final.columns:
                    df_final[col] = ""
            df_final = df_final[mapping_columns_lower]

       
            df_final = df_final.fillna("")

     
            mapping_ws.append_rows(df_final.values.tolist())
            logger.info(f"Appended {len(df_final)} rows to app_mapping_file successfully!")

        except Exception as e:
            logger.critical(f"Failed to append updated rows to app_mapping_file: {e}")
            print(f"Failed to append rows: {e}")


    def wait_until(self, hour, minute=0):
        now = datetime.now()
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target < now:
            target += timedelta(days=1)
        wait_seconds = (target - now).total_seconds()
        print(f" Waiting {int(wait_seconds)} seconds until {target}")
        time.sleep(wait_seconds)
    
    def run_daily(self):
        last_file = None

        while True:
            try:
                if not self.run_immediately:
                    self.wait_until(self.run_hour, self.run_minute)
                else:
                    print("RUN_IMMEDIATELY: Processing immediately")

                mail = self.connect_gmail()
                excel_file = self.get_latest_excel(mail)

                if excel_file:
                    if excel_file == last_file:
                        print(f"File '{excel_file}' already processed. Skipping...")
                        continue  # skip this file without waiting

                    print(f"Found new file: {excel_file}")
                    self.append_to_mapping_sheet(excel_file)
                    #os.remove(excel_file)  #deletion of temp file
                    last_file = excel_file
                    print(f"Task completed for: {excel_file}")
                else:
                    print("No new file found. Waiting 2 hours before next check")

                time.sleep(2 * 60 * 60)

            except Exception as e:
                logger.critical(f"Error in run_daily: {e}")
                print("An error occurred. Retrying in 10 minutes...")
                time.sleep(10 * 60) 

if __name__ == "__main__":
    updater = ExcelGSheetAppender()
    updater.run_daily()