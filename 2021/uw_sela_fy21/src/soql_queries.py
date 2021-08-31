student_list_query = """
SELECT 
Student__c, 
Student__r.HIGH_SCHOOL_GRADUATING_CLASS__c, 
Student__r.Age__c, 
student_audit_status__c, 
student__r.RecordType.Name, 
student__r.MailingPostalCode,
Student__r.Gender__c,
Student__r.Employment_Status__c,
Student__r.Annual_Household_Income__c,
GPA_HS_cumulative__c,
Student__r.Total_Community_Service_Hours_Completed__c,
Student__r.Four_Year_College_Applications__c,
Student__r.FA_Req_FAFSA__c,
Student__r.ACT_Superscore_highest_official__c,
Student__r.Region_Specific_Funding_Eligibility__c,
Student__r.Four_Year_College_Acceptances__c,
Student__r.Ethnic_Background__c,
Attendance_Rate__c,
RecordType.Name,
Student__r.Citizen_c__c,
Student__r.Most_Recent_GPA_Cumulative__c
FROM Academic_Semester__c
WHERE Global_Academic_Semester__r.Name LIKE '%Spring 2020-21%' AND 
Student__r.SITE__r.Name = 'College Track New Orleans' AND 
Student__r.AY_2020_21_Student_Served__c IN (
    'High School Student', 
    'Post-Secondary Student'
    )
"""


workshop_query = """
SELECT 
Student__c,
Academic_Semester__c,
Workshop_Display_Name__c,
Workshop_Dosage_Type__c,
Attendance_Numerator__c,
Attendance_Denominator__c,
Workshop_Enrollment__r.Status__c
FROM Class_Attendance__c
WHERE Academic_Semester__r.Global_Academic_Semester__r.Name LIKE '%Summer 2020-21%' AND 
Student__r.SITE__r.Name = 'College Track New Orleans' AND 
Student__r.AY_2020_21_Student_Served__c IN (
    'High School Student', 
    'Post-Secondary Student'
    ) AND 
(
    Workshop_Dosage_Type__c LIKE '%Math Blast%' OR 
    Workshop_Display_Name__c LIKE '%Summer Bridge%'
    )
"""


scholarship_query = """
SELECT 
Student__c,
RecordType.Name,
Status__c
FROM Scholarship_Application__c
WHERE Student__r.SITE__r.Name = 'College Track New Orleans' AND 
RecordType.Name = 'External' AND 
Status__c != NULL
"""

