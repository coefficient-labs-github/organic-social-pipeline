import funcs
import requests
import pandas as pd
import numpy as np
from google.cloud import bigquery
import gspread
from gspread_dataframe import get_as_dataframe

def main():
    # Fetch leads in Phantombuster database
    raw = funcs.PB.pb_fetch(open("../config/pb_link.txt", "r").read().strip())

    # Pull current post information from Google Sheets
    li_raw = funcs.PBBQDataProcessing.process_gspred(open("../config/sheets_key.json", "r").read().strip(), raw)

    # Process LinkedIn companies; create LinkedIn companies table
    li_raw, li_companies = funcs.PBBQDataProcessing.process_li_companies(li_raw)

    # Process LinkedIn contacts; create LinkedIn contacts (fact) table
    li_contacts = funcs.PBBQDataProcessing.process_li_contacts(li_raw, li_companies)

    # Process LinkedIn posts; create LinkedIn posts table
    li_posts = funcs.PBBQDataProcessing.process_li_posts(li_raw)

    # Subset data; Get tables with only new leads (leads that are not already in BigQuery)
    new_contacts, new_companies, new_posts = funcs.PBBQDataProcessing.subset_data(li_companies, li_contacts, li_posts)

    # Print new contacts to push
    print("-NEW ROWS TO PUSH-")
    print(new_contacts)
    print(new_companies)
    print(new_posts)
    print("\n")

    # Push data to BigQuery Tables: companies, contacts, posts
    funcs.BQ.bq_push_tables(
        "../config/skilled-tangent-448417-n8-35dde3932757.json",
        "skilled-tangent-448417-n8.pb_dataset",
        contacts=new_contacts,
        companies=new_companies,
        posts=new_posts)

if __name__ == "__main__":
    main()