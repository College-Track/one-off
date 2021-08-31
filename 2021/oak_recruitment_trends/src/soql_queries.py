applications = """
SELECT Id,  Gender__c, Indicator_Low_Income__c, Indicator_First_Generation__c, Last_College_Track_Application__r.CreatedDate, High_school_graduating_Class__c, College_Track_Status__c
FROM Contact
WHERE Site__c = '00146000014NGDGAA4'
AND Last_College_Track_Application__r.CreatedDate >= 2019-12-01T00:00:00.000Z
"""
