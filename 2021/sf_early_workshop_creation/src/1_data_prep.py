import pandas as pd
from simple_salesforce import Salesforce, format_soql
import os
from dotenv import load_dotenv
from ct_snippets.load_sf_class import SF_SOQL, SF_Report
from ct_snippets.sf_bulk import SF_Bulk
from reportforce import Reportforce
import numpy as np
import soql_queries as soql
import variables
from gspread_pandas import Spread, Client, conf


load_dotenv()

SPREADSHEET_ID = "18FOusPkZ2GoUy7vQ8L6borhvEDR8nIslXPpY3dX5prE"

SF_PASS = os.environ.get("SF_PASS")
SF_TOKEN = os.environ.get("SF_TOKEN")
SF_USERNAME = os.environ.get("SF_USERNAME")

sf = Salesforce(username=SF_USERNAME, password=SF_PASS, security_token=SF_TOKEN)
rf = Reportforce(session_id=sf.session_id, instance_url=sf.sf_instance)

# config = conf.get_config("./gspread_pandas/")

spread = Spread(SPREADSHEET_ID)
# , config=config)
# spread.open_sheet(0)

list_of_dfs = []

for i in range(6):
    spread.open_sheet(i)

    _df = spread.sheet_to_df(index=0)
    _workshops = list(_df.columns)[1:]
    _workshop_names = list(_df.iloc[1, 1:])
    num_workshops = len(_workshops)
    for workshop_num in range(num_workshops):
        student_list = [x for x in list(_df.iloc[2:, workshop_num + 1]) if x]
        _workshop_df = pd.DataFrame({"student_name": student_list})
        _workshop_df["workshop_id"] = _workshops[workshop_num]
        _workshop_df["workshop_name"] = _workshop_names[workshop_num]
        list_of_dfs.append(_workshop_df)


combined_df = pd.concat(list_of_dfs, ignore_index=True)

spread.open_sheet(6)
student_ids_df = spread.sheet_to_df(index=0)

prepped_df = combined_df.merge(
    student_ids_df, left_on="student_name", right_on="Full Name", how="left"
)

prepped_df = prepped_df.dropna(subset=["18 Digit ID"], axis=0)
# existing_sheet = spread.sheet_to_df(sheet="Sheet1")

# Sample for loading from SOQL
query = """
SELECT Id, Student__r.Full_Name__c, Class__r.Workshop_Display_Name__c, CreatedDate, CreatedById
FROM Class_Registration__c
WHERE Academic_Semester__r.Name LIKE '%Fall 2021-22%'
AND Student__r.Site__r.Name = 'College Track San Francisco'
AND CreatedById = '0051M000009602uQAA'
 """
sf_data = SF_SOQL("data", query)
sf_data.load_from_sf_soql(sf)
# sf_data.write_file()


test = prepped_df.merge(
    sf_data.df,
    left_on=["workshop_name", "Full Name"],
    right_on=["C_Workshop_Display_Name__c", "C_Full_Name__c"],
    how="left",
)

test = test[pd.isna(test["CR_Id"])]

# Sample for pushing data to SFDC

sf_upload = SF_Bulk(test)


sf_upload.data_dict = {"workshop_id": "Class__c", "18 Digit ID": "Student__c"}

sf_upload.generate_data_dict()

original_success = sf_upload.upload_df.copy()

second = sf_upload.upload_df.copy()

# Option to use either segmented uploads or one upload.
# Segmented Uploads:
sf_upload.process_segmented_upload(
    2,
    sf_object="Class_Registration__c",
    sf=sf,
    batch_size=5,
    use_serial=True,
    bulk_type="insert",
)

# One Upload:
sf_upload.sf_bulk(
    "Class_Registration__c", sf, bulk_type="insert", batch_size=2, use_serial=True
)

# Display the results of the upload
sf_upload.process_bulk_results()


sf_upload.fail_df[~pd.isna(sf_upload.fail_df["workshop_id"])]

df = pd.read_csv("~/Desktop/fail.csv")

