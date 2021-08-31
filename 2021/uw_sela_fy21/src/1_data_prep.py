import pandas as pd
from simple_salesforce import Salesforce, format_soql
import os
from dotenv import load_dotenv
from ct_snippets.load_sf_class import SF_SOQL, SF_Report
from ct_snippets.sf_bulk import sf_bulk, sf_bulk_handler, generate_data_dict
from reportforce import Reportforce
import numpy as np
import soql_queries as soql
import variables
from uszipcode import SearchEngine
import helpers


load_dotenv()
search = SearchEngine(simple_zipcode=True)

SF_PASS = os.environ.get("SF_PASS")
SF_TOKEN = os.environ.get("SF_TOKEN")
SF_USERNAME = os.environ.get("SF_USERNAME")

sf = Salesforce(username=SF_USERNAME, password=SF_PASS, security_token=SF_TOKEN)
# rf = Reportforce(session_id=sf.session_id, instance_url=sf.sf_instance)


# Sample for loading from SOQL
student_list = SF_SOQL("student_list_ay2020_21", soql.student_list_query)
student_list.load_from_sf_soql(sf)
student_list.write_file(subfolder="raw", file_type=".csv", file_level="child")

workshop_data = SF_SOQL("workshops_ay2020_21", soql.workshop_query)
workshop_data.load_from_sf_soql(sf)
workshop_data.write_file(subfolder="raw", file_type=".csv", file_level="child")


scholarship_data = SF_SOQL("scholarships_ay2020_21", soql.scholarship_query)
scholarship_data.load_from_sf_soql(sf)
scholarship_data.write_file(subfolder="raw", file_type=".csv", file_level="child")


# Joining data into master DF


workshop_grouped = (
    workshop_data.df.groupby(["CA_Student__c", "CA_Workshop_Dosage_Type__c"])
    .sum()
    .reset_index()
)

workshop_grouped["attendance_rate"] = (
    workshop_grouped.CA_Attendance_Numerator__c
    / workshop_grouped.CA_Attendance_Denominator__c
)

workshop_grouped["above_80"] = workshop_grouped.attendance_rate >= 0.8

workshop_grouped_subset = workshop_grouped[
    ["CA_Student__c", "attendance_rate", "above_80"]
]

scholarship_grouped = (
    scholarship_data.df.groupby("SA_Student__c")["SA_Status__c"].count().reset_index()
)

scholarship_grouped.rename(columns={"SA_Status__c": "num_scholarships"}, inplace=True)

df = student_list.df.merge(
    workshop_grouped_subset,
    left_on="AS_Student__c",
    right_on="CA_Student__c",
    how="left",
)


df = df.merge(
    scholarship_grouped, left_on="AS_Student__c", right_on="SA_Student__c", how="left"
)

# modifying fields in the main data frame

df["Parish"] = df.apply(
    lambda x: search.by_zipcode(x["C_MailingPostalCode"]).county, axis=1
)

df.loc[
    df["RT_RecordType__c"] == "High School Semester", "RT_RecordType__c"
] = "High School Student"
df.loc[
    df["RT_RecordType__c"] == "College/University Semester", "RT_RecordType__c"
] = "College Student"

df.loc[
    pd.isna(df.C_Employment_Status__c), "C_Employment_Status__c"
] = "No information available"

df["Income Bucket"] = df.apply(
    lambda x: helpers.create_income_bucket(x["C_Annual_household_income__c"]), axis=1
)
# saving results

df.to_pickle("../data/processed/merged_data_ay_2020_21.pkl")
