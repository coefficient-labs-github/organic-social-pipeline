# LinkedIn to HubSpot Lead Generation Pipeline

## 🚀 Overview
This project is a **fully automated and scheduled data pipeline** that scrapes LinkedIn post engagers, formats the data into a relational format, stores it in **BigQuery**, and pushes qualified leads into **HubSpot** for outreach. The pipeline has been modularized into separate Python scripts for better maintainability and scalability.

## 📊 Pipeline Architecture
![Pipeline Flowchart](media/organic_social_pipeline_flowchart.png)
- **Phantombuster API** → Scrapes LinkedIn post engagers.
- **Python Script: pb_bq.py** → Pulls and preprocesses PhantomBuster data into realtional DB, sends to BigQuery.
- **Google BigQuery** → Stores and processes raw lead data
- **Python Script: bq_hs.py** →Pulls BigQuery leads, formats them, and pushes the newest leads to Hubspo.
- **HubSpot API** → Pushes formatted lead data for sales outreach.
- **Sales Team** → Engages with leads and updates stages manually.

## 🛠️ Technologies Used
- **Python**
- **Phantombuster API**
- **Google BigQuery**
- **HubSpot API**
- **Numpy**
- **Pandas**
- **Requests**
- **Gspread**

## 📊 BigQuery Schema
![BigQuery Schema](media/BQ_schema.png)

## 📂 Project Structure
```
📁 organic_social_pipelins
│── 📁 etl_scripts/ 
│    ├── funcs.py                  # Helper functions for the ETL python scripts below
│    ├── pb_bq.py                  # Pulls/formats data from PhantomBuster; sends data to BigQuery
│    ├── bq_hs.py                  # Formats BigQuery data; sends data to HubSpot
│
│── 📁 docs/                       # Documentation providing examples of runs
│    ├── pb_bq_example_output.txt  # Example output: PhantomBuster -> BigQuery Pipeline
│    ├── bq_hs_example_output.txt  # Example Output: BigQuery -> Hubspot Pipeline
│
│── 📁 media/                      # Contains media (images and flowcharts for github_repo)
│    ├── ...
│
│─ .gitignore                       
│─ requirements.txt
│─ .env
│─ README.md       
```

## ⚡ Quickstart
### **1️⃣ Install Dependencies**
```bash
pip install -r requirements.txt
```

### **2️⃣ Set Up API Keys**
Ensure that all required API keys are present in the `.env/` file.

### **3️⃣ Run the Full Pipeline**
```bash
python etl_scripts/pb_bq.py
python etl_scripts/bq_hs.py
```
