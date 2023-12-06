'''
Skill Insight
'''
job_skill_insgt = spark.sql('SELECT COUNT(*) AS cnt, skill_abr, ROUND((COUNT(*) / SUM(COUNT(*)) OVER ()) * 100, 2) AS percentage_of_total FROM Linkedin.job_skills GROUP BY skill_abr ORDER BY cnt DESC LIMIT 20')


'''
Job Title Insight
'''
job_title_insgt = spark.sql('SELECT formatted_work_type, COUNT(*) AS cnt, ROUND((COUNT(*) / SUM(COUNT(*)) OVER ()) * 100, 2) AS percentage_of_total FROM Linkedin.job_postings GROUP BY formatted_work_type ORDER BY cnt DESC LIMIT 10')



'''
formatted_work_type Insight
'''
formatted_work_type_insgt = spark.sql('SELECT formatted_work_type, COUNT(*) AS cnt, ROUND((COUNT(*) / SUM(COUNT(*)) OVER ()) * 100, 2) AS percentage_of_total FROM Linkedin.job_postings GROUP BY formatted_work_type ORDER BY cnt DESC LIMIT 10')



'''
formatted_experience_level Insight
'''
experience_level_insgt = spark.sql('SELECT formatted_experience_level, COUNT(*) AS cnt, ROUND((COUNT(*) / SUM(COUNT(*)) OVER ()) * 100, 2) AS percentage_of_total FROM Linkedin.job_postings where formatted_experience_level is not null GROUP BY formatted_experience_level ORDER BY cnt DESC LIMIT 10')


'''
Company Industy Insight
'''
comp_industry_insgt = spark.sql('SELECT COUNT(*) AS cnt, industry, ROUND((COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY industry)) * 100, 2) AS percentage_of_total FROM Linkedin.company_industries a join Linkedin.companies b on a.company_id = b.company_id join Linkedin.job_postings c on b.company_id = c.company_id GROUP BY industry ORDER BY cnt DESC LIMIT 10')


'''
Company Location Insight
'''
comp_country_insgt = spark.sql("SELECT COUNT(*) AS cnt, country, ROUND((COUNT(*) / SUM(COUNT(*)) OVER ()) * 100, 2) AS percentage_of_total FROM Linkedin.companies where country <> '0' GROUP BY country ORDER BY cnt DESC LIMIT 10")



''''
Vizualization
'''

!pip install thrift
!pip install sasl
!pip install pyhive

from pyhive import hive

# Create a Hive connection
conn = hive.Connection(host="your_hive_host", port=10000, username="your_username")

# Create a cursor
cursor = conn.cursor()


'''
Creating Job Skill Visualization
'''
# Execute a Hive query
cursor.execute("SELECT * FROM Linkedin.job_skills")

# Fetch the results into a Pandas DataFrame
job_skill_df = cursor.fetchall()

job_skill_df["skill_abr"].value_counts().head(n=10).plot(kind="bar")
plt.xlabel("Skill Abbreviation")
plt.ylabel("count")
plt.title("Top 10 Skill Abbreviations")
plt.show()




'''
Creating Industry Visualization
'''
# Execute a Hive query
cursor.execute("SELECT * FROM Linkedin.company_industries")

# Fetch the results into a Pandas DataFrame
comp_indust_df = cursor.fetchall()

job_skill_df["industry"].value_counts().head(n=10).plot(kind="bar")
plt.xlabel("Industry")
plt.ylabel("count")
plt.title("Distribution of Top 10 Industries")
plt.show()


'''
Creating Industry Visualization
'''
# Execute a Hive query
cursor.execute("SELECT * FROM Linkedin.companies")

# Fetch the results into a Pandas DataFrame
comp_indust_df = cursor.fetchall()

job_skill_df["country"].value_counts().head(n=10).plot(kind="bar")
plt.xlabel("country")
plt.ylabel("count")
plt.title("Top 10 Countries with Highest Job Postings")
plt.show()