import pandas as pd
import requests
from google.cloud import bigquery
from bq_hs import hs_prepare_request, hs_fetch_list_contacts
import json

def apl_person_enrich(domain_url):
    url = f"https://api.apollo.io/api/v1/organizations/enrich?domain={domain_url}"


    headers = {
        "accept": "application/json",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "x-api-key": open("../config/apollo_company_enrichment_key.txt", "r").read().strip()
    }

    response = requests.post(url, headers=headers)

    return response.text

def main():
    # Prepare HubSpot Contact List request
    list_id = 246
    url = f"https://api.hubapi.com/contacts/v1/lists/{list_id}/contacts/all?property=email"
    api_key, headers, url = hs_prepare_request(url, "../config/hs_key.txt")

    # Fetch all contacts from the HubSpot list
    all_contacts = hs_fetch_list_contacts(api_key, headers, url, list_id)

    # Convert general dictionary to contact dataframe
    df = pd.DataFrame([all_contacts[i]["properties"] for i in range(0,len(all_contacts))])

    # Clean hs_linkedin_url from {value:link} to link
    df = df.map(lambda x: x["value"] if isinstance(x, dict) and "value" in x else x)

    # Drop NaN
    # df = df.dropna(subset=["email"])

    # Iterate through each row, enrich based on columns
    enriched = []
    for i, row in df.iterrows():
        print(row)
        if isinstance(row["email"], str):
            domain_url = row["email"].split("@")[1]
        else:
            domain_url = None

        enriched_contact_props = json.loads(apl_person_enrich(domain_url=domain_url))

        # Extract required fields safely
        organization = enriched_contact_props.get("organization", {})
        enriched.append({
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

    # Display the resulting DataFrame
    print(enriched_df)

    # Save df to csv
    enriched_df.to_csv("../data/apl_enriched_data.csv")

if __name__ == "__main__":
    main()
