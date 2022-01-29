import datapane as dp
import pandas as pd


filename = "data/processed/merged_data_ay_2020_21.pkl"
df = pd.read_pickle(filename)

gender_age_breakdown = pd.crosstab(
    [df.C_Gender__c, df.C_Age__c],
    df.RT_RecordType__c,
    margins=True,
    colnames=["Student Type"],
    rownames=["Gender", "Age"],
    margins_name="Total",
)

report = dp.Report(
    dp.Page(
        title="FY21 EOY",
        blocks=[
            "### Dataset",
            dp.Text(file="reports/fy21_eoy.md").format(
                gender_age_breakdown=gender_age_breakdown
            ),
            dp.Table(gender_age_breakdown),
        ],
    ),
)

report.upload(name="Test", open=True)
# executive_summary_pt1 = open("text/executive_summary_pt1.md", "r").read()
