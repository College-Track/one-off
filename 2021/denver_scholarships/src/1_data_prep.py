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


load_dotenv()


SF_PASS = os.environ.get("SF_PASS")
SF_TOKEN = os.environ.get("SF_TOKEN")
SF_USERNAME = os.environ.get("SF_USERNAME")

sf = Salesforce(username=SF_USERNAME, password=SF_PASS, security_token=SF_TOKEN)
rf = Reportforce(session_id=sf.session_id, instance_url=sf.sf_instance)

# Sample for loading from SOQL
query = """
SELECT Student__c, SUM(Amount__c) amount
FROM Bank_Book__c
WHERE Student__r.Site__r.Name = 'College Track Denver'
AND Student__r.Indicator_Completed_CT_HS_Program__c = true
AND Date__c >= 2020-08-01
AND Date__c < 2021-08-01
AND RecordType.Name = 'Bank Book Disbursements'
GROUP BY Student__c

 """


query_2 = """
SELECT 
Id, 
FirstName, 
LastName, 
Birthdate, 
Gender__c, 
Ethnic_background__c, 
MailingStreet, 
MailingCity, 
MailingState, 
MailingPostalCode, 
Phone, 
Email, 
Primary_Contact_Email__c, 
Primary_Contact_Mobile__c,
Current_School__c,
Current_Major__c,
FA_Obj_EFC_Calculation__c
FROM Contact
WHERE Site__r.Name = 'College Track Denver'
AND Indicator_Completed_CT_HS_Program__c = true


"""

query_3 = """
SELECT Student__c, School__r.Name
FROM Academic_Semester__c
WHERE Student__r.Site__r.Name = 'College Track Denver'
AND Term__c = 'Spring'
AND Grade__c = '12th Grade'
AND Student__r.Indicator_Completed_CT_HS_Program__c = true
"""

sf_data = SF_SOQL("data", query)
sf_data.load_from_sf_soql(sf)


sf_students = SF_SOQL("students", query_2)
sf_students.load_from_sf_soql(sf)

sf_schools = SF_SOQL("schools", query_3)
sf_schools.load_from_sf_soql(sf)


# sf_data.write_file()


df = sf_students.df.merge(
    sf_data.df, left_on="C_Id", right_on="AR_Student__c", how="right"
)

df = df.merge(sf_schools.df, left_on="C_Id", right_on="AS_Student__c", how="left")

df.to_csv("~/Desktop/co_scholarships.csv")
