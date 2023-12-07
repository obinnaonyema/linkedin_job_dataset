from os import name, truncate
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("project").getOrCreate()

from pyspark.sql.types import IntegerType,BooleanType,DateType
from pyspark.sql.functions import lower, col, sum, avg, first, max

# loading two csvs as pyspark dataframe
job_postings_df = (spark.read.option("multiline", "true").option("quote",'"').option("header", "true").option("escape", "\\").option("escape", '"').csv("file:///home/data/project/job_postings.csv"))
companies_df=spark.read.csv("file:///home/data/project/companies.csv", header=True)


# clean up column id in job_posting to float
job_postings_df = job_postings_df.withColumn("company_id",job_postings_df.company_id.cast(IntegerType()))
job_postings_df = job_postings_df.withColumn("views",job_postings_df.views.cast(IntegerType()))
job_postings_df = job_postings_df.withColumn("applies",job_postings_df.applies.cast(IntegerType()))
job_postings_df = job_postings_df.withColumn("max_salary",job_postings_df.max_salary.cast(IntegerType()))


# count the number of rows in each column
job_postings_df.count()
companies_df.count()


## jobs with the highest salaries
max_salary_df = job_postings_df.select("job_id", "pay_period", "description", "max_salary").groupBy('pay_period').max("max_salary")

job_postings_df.select("pay_period", "max_salary", "job_id").orderBy(col("max_salary").desc()).filter(job_postings_df.pay_period == "HOURLY").show(5)

max_salary_df = job_postings_df.groupBy('pay_period').agg(max("max_salary").alias("max_max_salary"))
max_salary_df.show(5, truncate=False)
max_salary_df = max_salary_df.filter(max_salary_df.pay_period.isNotNull())
max_salary_job_df = job_postings_df.join(max_salary_df, (max_salary_df.pay_period == job_postings_df.pay_period) & (max_salary_df.max_max_salary == job_postings_df.max_salary), "inner")
# this selects the description of the job postings for further analysis
max_salary_job_df = max_salary_job_df.select("description")
max_salary_job_df.show(5)
max_salary_df.write.csv("file:///home/data/project/max_salary.csv")




job_postings_df.createOrReplaceTempView("job_postings_view")
spark.sql("SELECT job_id, description, pay_period, max(max_salary) as max_maximum FROM job_postings_view GROUP BY pay_period").show(5)


# removing rows with null company id
job_postings_df = job_postings_df.filter(job_postings_df.company_id.isNotNull())
companies_df = companies_df.filter(companies_df.company_id.isNotNull())


# An Inner Join of job_posting_df an d companies_df
job_cols = job_postings_df.columns
company_cols = companies_df.columns

job_postings_df = job_postings_df.selectExpr([col + ' as job_' + col for col in job_cols])
companies_df = companies_df.selectExpr([col + ' as company_' + col for col in company_cols])
join_job_companies_df = job_postings_df.join(companies_df, job_postings_df.job_company_id == companies_df.company_company_id, "inner")
# check for null values in company_id column
join_job_companies_df.filter(join_job_companies_df.job_company_id.isNull()).select('job_company_id').show(5)



# rank companies with the highest number of job posting 
highest_job_posting = join_job_companies_df.groupby("job_company_id", "company_name").count()
highest_job_posting.orderBy(col('count').desc()).show(10)

# top 20 companies with the highest number of applicant grouped by Title of job
join_job_companies_df = join_job_companies_df.filter(join_job_companies_df.job_applies.isNotNull())
highest_applicant_posting = join_job_companies_df.groupby("job_company_id", "company_name", "job_title").sum("job_applies")
highest_applicant_posting.orderBy(col('sum(job_applies)').desc()).show(20, truncate=False)


# number of companies that have applicants for the job posting
join_job_companies_df.count()
join_job_companies_df.groupby("left_company_id","left_title","left_applies").count()

# views vs applies
view_vs_applicants = join_job_companies_df.groupBy("company_name","job_title").agg(avg("job_views").alias("avg_views"),avg("job_applies").alias("avg_applicants"))
view_vs_applicants.show(20, truncate=False)



# how many people applied regardless of application type
join_job_companies_df.groupby("title", "application_type").sum("left_applies")

