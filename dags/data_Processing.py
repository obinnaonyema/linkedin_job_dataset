from pyspark.sql.functions import col, sum

'''
Calculating missing values % for job postings
'''
job_postings_df = spark.read.table("Linkedin.companies")
# Assuming job_postings_df is your DataFrame
total_rows = job_postings_df.count()
missing_percentage_columns = [(col(column), (sum(col(column).isNull().cast("int")) / total_rows * 100).alias(column)) for column in job_postings_df.columns]

# Create a new DataFrame with the percentage of missing values for each column
missing_percentage_df = job_postings_df.agg(*[expr for _, expr in missing_percentage_columns])

# Show the percentage of missing values for each column
missing_percentage_df.show()


'''
Calculating missing values % for companies
'''
companies_df = spark.read.table("Linkedin.companies")
# Assuming job_postings_df is your DataFrame
total_rows = companies_df.count()
missing_percentage_columns = [(col(column), (sum(col(column).isNull().cast("int")) / total_rows * 100).alias(column)) for column in companies_df.columns]

# Create a new DataFrame with the percentage of missing values for each column
missing_percentage_df = companies_df.agg(*[expr for _, expr in missing_percentage_columns])

# Show the percentage of missing values for each column
missing_percentage_df.show()


'''
Calculating missing values % for company_industries
'''
company_industries_df = spark.read.table("Linkedin.company_industries")
# Assuming job_postings_df is your DataFrame
total_rows = company_industries_df.count()
missing_percentage_columns = [(col(column), (sum(col(column).isNull().cast("int")) / total_rows * 100).alias(column)) for column in company_industries_df.columns]

# Create a new DataFrame with the percentage of missing values for each column
missing_percentage_df = company_industries_df.agg(*[expr for _, expr in missing_percentage_columns])

# Show the percentage of missing values for each column
missing_percentage_df.show()


'''
Calculating missing values % for company_specialities
'''
company_specialities_df = spark.read.table("Linkedin.company_specialities")
# Assuming job_postings_df is your DataFrame
total_rows = company_specialities_df.count()
missing_percentage_columns = [(col(column), (sum(col(column).isNull().cast("int")) / total_rows * 100).alias(column)) for column in company_specialities_df.columns]

# Create a new DataFrame with the percentage of missing values for each column
missing_percentage_df = company_specialities_df.agg(*[expr for _, expr in missing_percentage_columns])

# Show the percentage of missing values for each column
missing_percentage_df.show()