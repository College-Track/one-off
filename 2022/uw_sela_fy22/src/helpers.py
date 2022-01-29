def create_income_bucket(value):
    if value < 11872:
        return "Less than or equal to 30% of Median Income"
    elif value < 19392:
        return "Between 30% of 50% of Median Income"
    elif value < 31265:
        return "Between 51% and 80% of Median Income"
    else:
        return "Greater than 80% of Median Income"
