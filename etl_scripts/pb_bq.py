import funcs
import requests
import pandas as pd
import numpy as np
from google.cloud import bigquery
import gspread
from gspread_dataframe import get_as_dataframe
import os
import json
from dotenv import load_dotenv

def main():
    # Get environment variables
    load_dotenv()
    pb_link = os.environ["PHANTOMBUSTER_LINK"]
    sheets_key = json.loads(os.environ["GOOGLE_SHEETS_CREDENTIALS"])  
    bq_credentials = json.loads(os.environ["BIGQUERY_CREDENTIALS"]) 
    bq_dataset = os.environ["BIGQUERY_DATASET"]

    # Fetch leads in Phantombuster database
    raw = funcs.PB.pb_fetch(pb_link)

    # Pull current post information from Google Sheets
    li_raw = funcs.PBBQDataProcessing.process_gspred(sheets_key, raw)

    # Process LinkedIn companies; create LinkedIn companies table
    li_raw, li_companies = funcs.PBBQDataProcessing.process_li_companies(li_raw)

    # Process LinkedIn contacts; create LinkedIn contacts (fact) table
    li_contacts = funcs.PBBQDataProcessing.process_li_contacts(li_raw, li_companies)

    # Process LinkedIn posts; create LinkedIn posts table
    li_posts = funcs.PBBQDataProcessing.process_li_posts(li_raw)

    # Subset data; Get tables with only new leads (leads that are not already in BigQuery)
    new_contacts, new_companies, new_posts = funcs.PBBQDataProcessing.subset_data(
        li_companies, 
        li_contacts, 
        li_posts,
        bq_dataset,
        bq_credentials
    )

    # Print new contacts to push
    print("-NEW ROWS TO PUSH-")
    print(new_contacts)
    print(new_companies)
    print(new_posts)
    print("\n")

    # Push data to BigQuery Tables: companies, contacts, posts
    funcs.BQ.bq_push_tables(
        bq_credentials,
        bq_dataset,
        contacts=new_contacts,
        companies=new_companies,
        posts=new_posts)

if __name__ == "__main__":
    main()