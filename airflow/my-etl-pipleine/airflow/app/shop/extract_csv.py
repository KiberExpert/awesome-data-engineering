import pandas as pd
from utils.snowflake_uploader import upload_to_snowflake
from dotenv import load_dotenv
import os 

load_dotenv()

def extract_csv():
    df = pd.read_csv(os.getenv("CSV_PATH"))  # csv data path
    upload_to_snowflake(df, "orders")
