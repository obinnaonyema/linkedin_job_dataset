'''
Creating companies table.
'''
companies_df = (spark.read
      .option("multiline", "true")
      .option("quote", '"')
      .option("header", "true")
      .option("escape", "\\")
      .option("escape", '"')
      .csv('/user/root/Linkedin/companies.csv')
)

companies_df.write.saveAsTable("Linkedin.companies") 

'''
Creating Job_postings table.
'''
job_postings_df = (spark.read
      .option("multiline", "true")
      .option("quote", '"')
      .option("header", "true")
      .option("escape", "\\")
      .option("escape", '"')
      .csv('/user/root/Linkedin/job_postings.csv')
)

job_postings_df.write.saveAsTable("Linkedin.job_postings") 

'''
Creating benefits table.
'''
benefits_df = (spark.read
      .option("multiline", "true")
      .option("quote", '"')
      .option("header", "true")
      .option("escape", "\\")
      .option("escape", '"')
      .csv('/user/root/Linkedin/benefits.csv')
)

benefits_df.write.saveAsTable("Linkedin.benefits") 

'''
Creating company_industries table.
'''
company_industries_df = (spark.read
      .option("multiline", "true")
      .option("quote", '"')
      .option("header", "true")
      .option("escape", "\\")
      .option("escape", '"')
      .csv('/user/root/Linkedin/company_industries.csv')
)

company_industries_df.write.saveAsTable("Linkedin.company_industries") 

'''
Creating company_specialities table.
'''
company_specialities_df = (spark.read
      .option("multiline", "true")
      .option("quote", '"')
      .option("header", "true")
      .option("escape", "\\")
      .option("escape", '"')
      .csv('/user/root/Linkedin/company_specialities.csv')
)

company_specialities_df.write.saveAsTable("Linkedin.company_specialities") 
                                                                                                                                                                        
'''
Creating employee_counts table.
'''
employee_counts_df = (spark.read
      .option("multiline", "true")
      .option("quote", '"')
      .option("header", "true")
      .option("escape", "\\")
      .option("escape", '"')
      .csv('/user/root/Linkedin/employee_counts.csv')
)

employee_counts_df.write.saveAsTable("Linkedin.employee_counts") 

'''
Creating job_industries table.
'''                                                                                                                                                                     
job_industries_df = (spark.read
      .option("multiline", "true")
      .option("quote", '"')
      .option("header", "true")
      .option("escape", "\\")
      .option("escape", '"')
      .csv('/user/root/Linkedin/job_industries.csv')
)

job_industries_df.write.saveAsTable("Linkedin.job_industries") 

'''
Creating job_skills table.
'''                                                                                                                                                                          
job_skills_df = (spark.read
      .option("multiline", "true")
      .option("quote", '"')
      .option("header", "true")
      .option("escape", "\\")
      .option("escape", '"')
      .csv('/user/root/Linkedin/job_skills.csv')
)

job_skills_df.write.saveAsTable("Linkedin.job_skills")  
                                                                                                                                                                                                                                                                                                                                                          
     