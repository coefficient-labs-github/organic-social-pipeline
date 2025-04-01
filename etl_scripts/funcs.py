import requests
import pandas as pd
from google.cloud import bigquery
from io import BytesIO
import gspread
import json

class PBBQDataProcessing:
    """Helper functions for Data Processing: PhantomBuster to BigQuery Pipeline"""
    def process_gspred(gspread_credentials, raw):
        """Requests data from a Google Sheet, performs data processing, and returns a DataFrame."""
        # Get LinkedIn postIDs and names
        gc = gspread.service_account_from_dict(gspread_credentials)
        links = pd.DataFrame(gc.open("Organic Social Dashboard").worksheet('LI Links').get_all_values(), columns=['link', 'post', 'id'])

        # Merge raw and links to get postId and postName variable
        li_raw = raw.merge(links, left_on='postUrl', right_on='link', how='left').rename(columns={'post':'postName', 'id':'postId'}).drop(columns=['link'])

        # Add platform column
        li_raw["platform"] = "LinkedIn"

        return li_raw
    
    def process_li_companies(li_raw):
        """Processes Company Data from LinkedIn Raw DataFrame, returns LI raw data and LI companies table."""
        # Create a companyId for each company
        li_raw["companyId"] = li_raw["companyName"].str.strip().str.replace(r"[ ,.]", "", regex=True).str.lower()

        # Companies table
        li_companies = li_raw.loc[:,["companyId", "companyName", "companyUrl", "followersCount"]].dropna().drop_duplicates(subset=["companyUrl"])

        return li_raw, li_companies
    
    def process_li_contacts(li_raw, li_companies):
        """Processes Contact Data from LinkedIn Raw DataFrame, returns LI contacts table."""
        # Construct LinkedIn Contacts table
        li_contacts = li_raw.loc[:,["sourceUserId", "name", "occupation", "profileLink", "degree", "companyUrl", "postId", "reactionType", "platform"]]

        # Add comapny ID to LI Contacts Table
        li_contacts["companyId"] = li_companies["companyId"]

        # Drop duplicates
        li_contacts = li_contacts.drop_duplicates(subset=["profileLink"])

        # Drop companies
        li_contacts = li_contacts[li_contacts["companyId"].isna()]

        return li_contacts
    
    def process_li_posts(li_raw):
        """Processes post data from LinkedIn, returns LinkedIn post table."""
        # Construct LinkedIn Post table and drop duplicates
        li_posts = li_raw.loc[:, ["postUrl", "platform", "postId", "postName"]].drop_duplicates(subset=["postUrl"])

        return li_posts
    
    def subset_data(li_companies, li_contacts, li_posts, bq_dataset, bq_credentials):
        """Subsets dataframes across platforms, returns new contacts, companies, and posts."""
        # Query bigquery for current tables
        bq_tables = {}
        for t in ["contacts", "companies", "posts"]:
            query = f"SELECT * FROM `{bq_dataset}.{t}`;"
            bq_tables[t] = BQ.bq_query_table(bq_credentials, query)

        # Stack dataframes across platforms
        contacts = li_contacts
        companies = li_companies
        posts = li_posts

        # Subset the new leads to upload to BigQuery
        new_contacts = contacts.loc[~contacts["profileLink"].isin(bq_tables["contacts"]["profileLink"])]
        new_companies = companies.loc[~companies["companyId"].isin(bq_tables["companies"]["companyId"])]
        new_posts = posts.loc[~posts["postUrl"].isin(bq_tables["posts"]["postUrl"])]

        return new_contacts, new_companies, new_posts

class PB:
    """Helper functions for Phantombuster: Importing Scraped Leads."""
    def pb_fetch(dl_link):
        return pd.read_csv(BytesIO(requests.get(dl_link).content))

class HS:
    """Helper functions for HubSpot: Importing and Exporting Contacts."""
    def hs_prepare_request(url, api_key):

        # Headers for authentication
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        return api_key, headers, url

    def hs_fetch_list_contacts(headers, url):
        contacts = []
        params = {
            "count": 100
        }

        while True:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code != 200:
                print(f"Error: {response.status_code}, {response.text}")
                break

            data = response.json()

            # Extract contacts and add to the list
            if "contacts" in data:
                contacts.extend(data["contacts"])

            # Check for pagination (if there are more contacts to fetch)
            if "vid-offset" in data and data.get("has-more", False):
                params["vidOffset"] = data["vid-offset"]  # Update pagination parameter
            else:
                break

        return contacts

    def parse_hubspot_contacts(response):
        """
        Extracts vid and all properties from a list of HubSpot contacts (v1 API).
        """
        contacts_list = []

        all_properties = [
            "firstname",
            "lastname",
            "email",
            "company",
            "createdate",
            "organic_social_stage",
            "organic_social_outreached",
            "linkedin_profile_url_organic_social_pipeline",
            "latest_funding_date",
            "latest_funding_stage",
            "total_funding",
            "post_id",
            "post"
        ]

        for contact in response:
            parsed_data = {"vid": contact.get("vid")}  # Extract vid

            # Extract all requested properties, setting None if missing
            if "properties" in contact:
                for prop in all_properties:
                    if prop in contact["properties"]:
                        parsed_data[prop] = contact["properties"][prop].get("value")
                    else:
                        parsed_data[prop] = None  # Ensures consistency across all rows

            contacts_list.append(parsed_data)  # Add to list

        return pd.DataFrame(contacts_list)

    def hs_push_contacts_to_list(api_key, new_leads):
        """Pushes all contacts from a DataFrame to HubSpot."""

        url = "https://api.hubapi.com/crm/v3/objects/contacts"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

        # Check if there are leads to push
        if len(new_leads) > 0:

            for _, row in new_leads.iterrows():  # Iterate over DataFrame rows
                hubspot_record = {
                    "properties": {
                        "post_id": row.get("postId", ""),
                        "reaction_type": row.get("reactionType", ""),
                        "platform": row.get("platform", ""),
                        "company_id": str(row.get("companyId", "")),  # Convert to string if necessary
                        "post_name": row.get("postName", ""),
                        "firstname": row.get("name").split(" ")[0] if isinstance(row.get("name"), str) else "",
                        "lastname": " ".join(row.get("name").split(" ")[1:]) if isinstance(row.get("name"), str) else "",
                        "jobtitle": row.get("occupation", ""),
                        "linkedin_profile_url_organic_social_pipeline": row.get("profileLink", ""),
                        "hs_linkedin_url": row.get("profileLink", ""),
                        "pb_linkedin_profile_url": row.get("profileLink", ""),
                        "phantombuster_source_user_id": str(row.get("sourceUserId", ""))
                    }
                }

                response = requests.post(url, headers=headers, json=hubspot_record)

                if response.status_code == 201:
                    print(f"Successfully pushed: {row.get('name')}")
                else:
                    print(f"Failed to push: {row.get('name')}, Error: {response.text}")

        else:
            print("No new leads pushed")
    
    def hs_update_funding_details(api_key, funding_df):
        """Pushes all funding details from a DataFrame to HubSpot."""
        url_base = "https://api.hubapi.com/crm/v3/objects/contacts/"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

        # Check if there are leads to push
        if not funding_df.empty:
            for _, row in funding_df.iterrows():
                contact_id = row.get("vid")

                # Skip rows with missing contact IDs
                if pd.isna(contact_id):
                    print("Skipping row. Contact ID is missing.")
                    continue

                # Specify properties to update
                properties = {}
                company_name = row.get("company_name", "")
                if company_name: 
                    properties["company"] = str(company_name)

                crunchbase_url = row.get("crunchbase_url", "In Crunchbase (URL not listed)")
                properties["crunchbase_url"] = str(crunchbase_url)

                total_funding = row.get("total_funding")
                if pd.notna(total_funding): #Only add if the value is a number.
                    properties["total_funding"] = str(total_funding)

                latest_funding_stage = row.get("latest_funding_stage", "Venture backed (funding stage not listed)")
                properties["latest_funding_stage"] = str(latest_funding_stage)

                annual_revenue = row.get("annual_revenue", "Venture Backed (annual rev not listed)")
                properties["annualrevenue"] = str(annual_revenue)

                state = row.get("state", "")
                properties["state"] = str(state)

                # Construct the payload in the correct format
                payload = json.dumps({"properties": properties})

                # Try to update each contact
                contact_url = f"{url_base}{contact_id}"
                try:
                    response = requests.patch(contact_url, headers=headers, data=payload)
                    response.raise_for_status()

                    print(f"Successfully updated contact ID: {contact_id}")

                except requests.exceptions.RequestException as e:
                    print(f"Failed to update contact ID: {contact_id}, Error: {e}, Response: {response.text if 'response' in locals() else 'No Response'}")
        else:
            print("No contacts to update.")

class BQ:
    def bq_query_table(credentials, query):
        """Queries a BigQuery table and returns a pandas DataFrame."""
        client = bigquery.Client.from_service_account_info(credentials)
        query_job = client.query(query)
        results = query_job.result()
        return results.to_dataframe()
    
    def bq_push_tables(credentials, dataset_id, **kwargs):
        """Pushes all tables from a DataFrame to BigQuery."""
        client = bigquery.Client.from_service_account_info(credentials)

        # Define dataset and table
        dataset_id = dataset_id

        # Upload each table
        for table_name, df in kwargs.items():
            table_id = f"{dataset_id}.{table_name}"

            # Load the DataFrame into BigQuery
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                autodetect=True
            )

            # Wait for the load job to complete
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()

            # Confirm the load
            print(f"Loaded {job.output_rows} rows into {table_name}.")

        return None
    
class AP:
    def apl_person_enrich(domain_url, api_key):
        url = f"https://api.apollo.io/api/v1/organizations/enrich?domain={domain_url}"

        headers = {
            "accept": "application/json",
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "x-api-key": api_key
        }

        response = requests.post(url, headers=headers)

        return response.text