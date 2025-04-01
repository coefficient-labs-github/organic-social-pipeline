import pandas as pd
import requests
from google.cloud import bigquery
import funcs
import json
from dotenv import load_dotenv
import os

def main():
    # Prepare HubSpot Contact List request
    load_dotenv()
    list_id = 246
    hs_api_key = os.environ["HUBSPOT_API_KEY"]
    url = f"https://api.hubapi.com/contacts/v1/lists/{list_id}/contacts/all?property=hs_linkedin_url&property=email&property=vid"
    hs_api_key, headers, url = funcs.HS.hs_prepare_request(url, hs_api_key)

    # Fetch all contacts from the HubSpot list
    all_contacts = funcs.HS.hs_fetch_list_contacts(headers, url)
    
    # Initialize cleaned_property_list and vids
    cleaned_properties_list = []
    vids = []

    for contact in all_contacts:
        properties = {}
        for key, value_dict in contact["properties"].items():
            properties[key] = value_dict["value"]
        cleaned_properties_list.append(properties)
        vids.append(contact["vid"])

    # Convert properties to DataFrame
    df = pd.DataFrame(cleaned_properties_list)
    df["vid"] = vids
    
    # Drop NaN
    df = df.dropna(subset=["email"])

    # Prepare Apollo API Key
    ap_api_key = os.environ["APOLLO_COMPANY_ENR_KEY"]

    # Iterate through each row, enrich based on columns
    enriched = []
    for i, row in df[:20].iterrows():
        if isinstance(row["email"], str):
            domain_url = row["email"].split("@")[1]
        else:
            domain_url = None

        enriched_contact_props = json.loads(funcs.AP.apl_person_enrich(domain_url=domain_url, api_key=ap_api_key))

        # Extract required fields safely
        organization = enriched_contact_props.get("organization", {})
        enriched.append({
            "vid": row["vid"],
            "company_name": organization.get("name"),
            "crunchbase_url": organization.get("crunchbase_url"),
            "total_funding": organization.get("total_funding"),
            "latest_funding_stage": organization.get("latest_funding_stage"),
            "annual_revenue": organization.get("annual_revenue"),
            "latest_funding_date": organization.get("latest_funding_round_date"),
            "state": organization.get("state")
        })

    # Convert to DataFrame
    enriched_df = pd.DataFrame(enriched)

    # Save to csv (for testing)
    #enriched_df.to_csv("../temp_data/temp_enr_data.csv")

    # Update contact funding details in HubSpot
    #enriched_df = pd.read_csv("../temp_data/temp_enr_data.csv")
    funcs.HS.hs_update_funding_details(hs_api_key, enriched_df)    

if __name__ == "__main__":
    main()
