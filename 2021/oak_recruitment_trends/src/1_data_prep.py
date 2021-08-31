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
from datetime import datetime, timezone


load_dotenv()


SF_PASS = os.environ.get("SF_PASS")
SF_TOKEN = os.environ.get("SF_TOKEN")
SF_USERNAME = os.environ.get("SF_USERNAME")

sf = Salesforce(username=SF_USERNAME, password=SF_PASS, security_token=SF_TOKEN)
rf = Reportforce(session_id=sf.session_id, instance_url=sf.sf_instance)


# Sample for loading from SOQL
# query = """ """
sf_data = SF_SOQL("data", soql.applications)
sf_data.load_from_sf_soql(sf)
sf_data.date_columns = "CTA_CreatedDate"
sf_data.df.loc[:, "CTA_CreatedDate"] = pd.to_datetime(sf_data.df.CTA_CreatedDate)
# sf_data.write_file()


sf_data.df

admited_status = ["18a", "11A", "12A", "13A", "15A", "16A", "17A"]


sf_data.df["admitted"] = sf_data.df.C_College_Track_Status__c.isin(admited_status)


def determine_cycle(date):
    if date <= datetime(2020, 10, 31, tzinfo=timezone.utc):
        return "Dec 2019 - Oct 2020"
    else:
        return "November 2020 - June 2021"


sf_data.df["recruitment_cycle"] = sf_data.df.apply(
    lambda x: determine_cycle(x["CTA_CreatedDate"]), axis=1
)

sf_data.df["male_student"] = sf_data.df.C_Gender__c.isin(["Male"])

sf_data.df["low_income"] = sf_data.df.C_Indicator_Low_Income__c.isin(["Yes"])

male = pd.crosstab(
    [sf_data.df.recruitment_cycle, sf_data.df.male_student],
    sf_data.df.admitted,
    margins=True,
)

low_income = pd.crosstab(
    [sf_data.df.recruitment_cycle, sf_data.df.low_income],
    sf_data.df.admitted,
    margins=True,
)

first_gen = pd.crosstab(
    [sf_data.df.recruitment_cycle, sf_data.df.C_Indicator_First_Generation__c],
    sf_data.df.admitted,
    margins=True,
)

overall = pd.crosstab([sf_data.df.recruitment_cycle], sf_data.df.admitted, margins=True)

overall_percent = pd.crosstab(
    [sf_data.df.recruitment_cycle], sf_data.df.admitted, margins=True, normalize="index"
)


filename = "../data/processed/application_data" + ".xlsx"
writer = pd.ExcelWriter(filename, engine="xlsxwriter")

overall.to_excel(writer, index=True, sheet_name="Overall Applications")

overall_percent.to_excel(
    writer,
    index=True,
    sheet_name="Overall Applications",
    startcol=7,
    float_format="%.2f",
)

male.to_excel(writer, index=True, sheet_name="Targets")

low_income.to_excel(writer, index=True, sheet_name="Targets", startcol=6)

first_gen.to_excel(writer, index=True, sheet_name="Targets", startrow=10)

writer.save()


# Sample for pushing data to SFDC

# sf_upload = SF_Bulk(sf_data.df)

# sf_upload.data_dict = {
#     "C_Id": "Id",
# }

# sf_upload.generate_data_dict()

# Option to use either segmented uploads or one upload.
# Segmented Uploads:
# sf_upload.process_segmented_upload(5, sf_object="Contact", sf=sf)

# One Upload:
# sf_upload.sf_bulk("Task", sf, bulk_type="insert")

# Display the results of the upload
# sf_upload.process_bulk_results()
