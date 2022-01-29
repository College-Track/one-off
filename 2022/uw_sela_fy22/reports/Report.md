---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.5.2
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# FY21 United Way of Southeast Louisiana (UWSELA) Interim Report Data Request


For ticket #9558 submitted January 15, 2021.

Prepared by Baker Renneckar

```python
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd
from datetime import datetime
import numpy as np
# from tabulate import tabulate
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

```

```python
%matplotlib inline
```

```python
df = pd.read_pickle('../data/processed/merged_data.pkl')
```

## Demographic Data




#### Gender / Age / Student Type Breakdown

```python
pd.crosstab([df.C_Gender__c, df.C_Age__c], df.RT_RecordType__c, margins=True, colnames=[
            'Student Type'], rownames=['Gender', 'Age'], margins_name='Total')
```

#### Gender / Ethnic / Student Type Breakdown

```python hide_input=false
pd.crosstab([df.C_Gender__c, df.C_Ethnic_background__c], df.RT_RecordType__c, margins=True, colnames=[
            'Student Type'], rownames=['Gender', 'Ethnicity'], margins_name='Total')
```

#### Count of unduplicated participants by sex and Employment Status


```python
pd.crosstab([df.C_Gender__c, df.C_Employment_Status__c], df.RT_RecordType__c, margins=True, colnames=[
            'Student Type'], rownames=['Gender', 'Employment Status'], margins_name='Total')
```

#### *Count of unduplicated participants by household income in the following categories: Total Number of Persons less than or equal to 30% Area Median Income (extremely low income), Total Number of Persons over 30% not greater than 50% Area Median Income (low income), Total Number of Persons over 50% not greater than 80% Area Median Income (moderate income), Total Number of Persons over 80% Area Median Income (non-LMI), No Income, Unknown.

```python
pd.crosstab([df['Income Bucket']], df.RT_RecordType__c, margins=True, colnames=[
            'Student Type'], rownames=['Income Bucket'], margins_name='Total')
```

#### Gender / Parish Breakdown

The totals here don't add up to the above numbers due to us not having accurate location data on some students.

```python
pd.crosstab([df.C_Gender__c, df.Parish.fillna('Missing')], df.RT_RecordType__c, margins=True, colnames=[
            'Student Type'], rownames=['Gender', 'Parish'], margins_name='Total')
```

## High School Questions

```python
df_2021 = df[df.C_HIGH_SCHOOL_GRADUATING_CLASS__c == "2021"]
```

#### *% and # of Class of 2021 NOLA seniors who have submitted, completed, or are in the “review” phase with FAFSA to date

```python
def create_count_percent_cross_tab(df, row_column, column_column, row_name, subset_name = 'High School Student'):
    cross_1 = pd.crosstab(df[row_column].fillna('No Data'), df[column_column], dropna=False, colnames=[
            subset_name], rownames=[row_name], margins=True)
    
    cross_2 = pd.crosstab(df[row_column].fillna('No Data'), df[column_column], dropna=False, colnames=[
            subset_name], rownames=[row_name], normalize=True, margins=True)
    
    cross_1['Percent'] = cross_2[subset_name].values
    cross_1.rename(columns={subset_name: "Count"}, inplace=True)
    cross_1.drop('All', inplace=True, axis=1)
    return cross_1.style.format({'Percent': "{:.0%}"})

```

```python
df_2021_us_citizen = df_2021[df_2021.C_Citizen_c__c == 'US Citizen']
df_2021_us_citizen_pr = df_2021[df_2021.C_Citizen_c__c != 'Other']
```

Not Including permanent residence.

```python
create_count_percent_cross_tab(df_2021_us_citizen, 'C_FA_Req_FAFSA__c', 'RT_RecordType__c', 'FAFSA Status')
```

Including permanent residence.

```python
create_count_percent_cross_tab(df_2021_us_citizen_pr, 'C_FA_Req_FAFSA__c', 'RT_RecordType__c', 'FAFSA Status')
```

#### % and # of our Class of 2021 NOLA high school seniors who qualify for TOPS eligibility.

The first table is using data from Salesforce. The second table is just showing the students who are above or equal to a 2.5 GPA and 20 ACT

```python
create_count_percent_cross_tab(df_2021, 'C_Region_Specific_Funding_Eligibility__c', 'RT_RecordType__c', 'TOPS Status')
```

```python
df_2021['tops_proxy'] = np.where((df_2021.C_Most_Recent_GPA_Cumulative__c >= 2.5)&(df_2021.C_ACT_Superscore_highest_official__c >=20),True,False)
```

```python
create_count_percent_cross_tab(df_2021, 'tops_proxy', 'RT_RecordType__c', 'TOPS Status - Proxy')
```

#### # and % of four-year college acceptances for the high school Class of 2021 during the reporting period.


```python
df_2021['accepted_college'] = df.C_Four_Year_College_Acceptances__c > 0
```

```python
create_count_percent_cross_tab(df_2021, 'accepted_college', 'RT_RecordType__c', 'Accepted into College')
```

#### # and % of NOLA high school students in the 2020-21 academic year with 3.0+ cumulative GPAs

The first table is based on the most recent GPA value we have on file for students. Which is a mix of Spring 2019-20 and Fall 2020-21 data. 

The second table is based only Fall 2020-21 data, which is mostly incomplete and will be until February.

```python
df_hs = df[df.RT_RecordType__c == "High School Student"]

```

```python
df_hs['above_3_gpa_most_recent'] = df.C_Most_Recent_GPA_Cumulative__c >= 3.0
df_hs['above_3_gpa_fall'] = df.AS_GPA_HS_cumulative__c >= 3.0
```

```python
create_count_percent_cross_tab(df_hs, 'above_3_gpa_most_recent', 'RT_RecordType__c', 'Most Recent GPA >= 3.0')
```

```python
create_count_percent_cross_tab(df_hs, 'above_3_gpa_fall', 'RT_RecordType__c', 'Fall 2020-21 GPA >= 3.0')
```

#### % of NOLA freshmen and sophomores required to attend MathBlast who completed the 3-5 weeks during this reporting period (summer 2020)

 I don't have a easy way to determine who is required to attend Math Blast so, I'm assuming if a student attended any Math Blast they were required to attend, and if they didn't attend anything they weren't required (or had a waver). I'm also using attended 80%+ as a proxy for 3-5 weeks.Let me know if you think we should do something else. 

```python
df_2022_2023 = df[(df.C_HIGH_SCHOOL_GRADUATING_CLASS__c == "2023") | (df.C_HIGH_SCHOOL_GRADUATING_CLASS__c == "2022")]
```

```python
df_2022_2023 = df_2022_2023[df_2022_2023.attendance_rate > 0]
```

```python
create_count_percent_cross_tab(df_2022_2023, 'above_80', 'RT_RecordType__c', 'Attended 80%+ Math Blast')
```

#### average # of four-year colleges to which Class of 2021 NOLA high school seniors applied during this reporting period



```python
df_2021.C_Four_Year_College_Applications__c.mean().round(2)
```

#### average # of outside scholarships for which Class of 2021 NOLA high school seniors applied during this reporting period

```python
df_2021.num_scholarships.mean().round(2)
```

#### # of avg. community service hours Class of 2021 NOLA high school seniors have completed throughout high school as of December 31, 2020

```python
df_2021.C_Total_Community_Service_Hours_Completed__c.mean().round(2)
```

#### % of NOLA high school students who attend 80% or more of their scheduled College Track programming sessions during this reporting period

```python
df_hs['above_80_overall'] = df_hs.AS_Attendance_Rate__c >= 80
```

```python
create_count_percent_cross_tab(df_hs, 'above_80_overall', 'RT_RecordType__c', 'Attended 80%+ Fall 2020-21 Workshops')
```

## College Questions


#### % of rising college freshmen who completed "College Prep Institute" to prepare for matriculation during this reporting period

I don't see any workshops with "College Prep Institute" in the title since 2016. They do have Summer Bridge, which I assume is the same thing.

```python
df_2020 = df[(df.C_HIGH_SCHOOL_GRADUATING_CLASS__c == "2020")]
```

```python
create_count_percent_cross_tab(df_2020, 'above_80', 'RT_RecordType__c', 'Attended 80%+ Summer Bridge',subset_name='College Student')
```

## Questions with No Data Related Answer


**Question:**

* The number of households that fit within the following "Client Type of Household" categories: Couple With Children/Dependents, Couple With No Children/Dependents, Female Householder With Children/Dependents - No Spouse/Partner, Male Householder with Children/Dependents - No Spouse/Partner, Single Household, Other, Unknown
--from our last report, I understand most if not all of our student households fall into the "unknown" category. Please confirm that this is still the case.

**Answer: All of our students would fall into the unknown category**

<!-- #region -->
**Question:** # and % of high school students promoted to the next grade from the 2019-2020 academic year



**Answer: 100% of our students we still track would have moved on a grade and we don't have a way to track students who left the program.**
<!-- #endregion -->

<!-- #region -->
**Question:** *% and # of class of 2020 seniors who graduated high school on time




**Answer: Likewise, we have to say 100% of our students graduated HS because if a student wasn't going to graduate HS then they would be removed from the program.**
<!-- #endregion -->

```html

<script>
$(document).ready(function(){
    window.code_toggle = function() {
        (window.code_shown) ? $('div.input').hide(250) : $('div.input').show(250);
        window.code_shown = !window.code_shown
    }
    if($('body.nbviewer').length) {
        $('<li><a href="javascript:window.code_toggle()" title="Show/Hide Code"><span class="fa fa-code fa-2x menu-icon"></span><span class="menu-text">Show/Hide Code</span></a></li>').appendTo('.navbar-right');
        window.code_shown=false;
        $('div.input').hide();
    }
});
</script>


<style>

div.prompt {display:none}


h1, .h1 {
    font-size: 33px;
    font-family: "Trebuchet MS";
    font-size: 2.5em !important;
    color: #2a7bbd;
}

h2, .h2 {
    font-size: 10px;
    font-family: "Trebuchet MS";
    color: #2a7bbd; 
    
}


h3, .h3 {
    font-size: 10px;
    font-family: "Trebuchet MS";
    color: #5d6063; 
    
}

.rendered_html table {

    font-size: 14px;
}

.output_png {
  display: flex;
  justify-content: center;
}

.cell {
    padding: 0px;
}


</style>
```
