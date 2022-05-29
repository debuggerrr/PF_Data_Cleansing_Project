import io
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import numpy as np

spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.memory", "8g") \
    .appName('my-cool-app') \
    .getOrCreate()

df = spark.read.option("header", "true").option("multiline", "true").option("escape", "\"").csv(
    "/home/lumiq/Documents/salary_data/data_3.csv").selectExpr("Timestamp", "Employer", "Location", "`Job Title`",
                                                               "`Years of Experience`",
                                                               "`Annual Base Pay`", "`Signing Bonus`", "`Annual Bonus`",
                                                               "Gender")  # Selecting subset of columns of data ONLY as per the problem statement.

df.printSchema()

new_columns_list = ["Created_At", "Company_Name", "Location", "Job_Title",
                    "Work_Exp", "Annual_Base_Salary", "Signing_Bonus", "Annual_Bonus",
                    "Gender"]  # New Columns Names List
renamed_df = df.toDF(*new_columns_list)

trimmed_data = ""
for i in renamed_df.columns:
    trimmed_data = renamed_df.withColumn(i, trim(
        col(i)))  # Removing of leading and trailing spaces from the dataset column values.

updated_for_null = trimmed_data.withColumn("Created_At",
                                           when(col("Created_At").isNull(), lit("01/01/1900 00:00:00")).otherwise(
                                               col("Created_At"))).withColumn("Company_Name",
                                                                              when(col("Company_Name").isNull(),
                                                                                   lit("")).otherwise(
                                                                                  col("Company_Name"))).withColumn(
    "Location",
    when(col("Location").isNull(), lit("")).otherwise(
        col("Location"))).withColumn("Job_Title",
                                     when(col("Job_Title").isNull(), lit("")).otherwise(
                                         col("Job_Title"))).withColumn(
    "Work_Exp",
    when(col("Work_Exp").isNull(), lit("0")).otherwise(
        col("Work_Exp"))).withColumn("Annual_Base_Salary",
                                     when(col("Annual_Base_Salary").isNull(), lit("0")).otherwise(
                                         col("Annual_Base_Salary"))).withColumn("Signing_Bonus",
                                                                                when(col("Signing_Bonus").isNull(),
                                                                                     lit("0")).otherwise(
                                                                                    col("Signing_Bonus"))).withColumn(
    "Annual_Bonus",
    when(col("Annual_Bonus").isNull(), lit("0")).otherwise(
        col("Annual_Bonus"))).withColumn("Gender",
                                         when(col("Gender").isNull(), lit("")).otherwise(
                                             col("Gender")))  # Handling NULL cases with default values.

clean_amount_columns = updated_for_null.withColumn("Annual_Base_Salary",
                                                   regexp_replace(col("Annual_Base_Salary"), "\$", "")).withColumn(
    "Annual_Base_Salary",
    regexp_replace(col("Annual_Base_Salary"), "\€", "")).withColumn(
    "Annual_Base_Salary",
    regexp_replace(col("Annual_Base_Salary"), ",", "")).withColumn("Annual_Base_Salary",
                                                                   regexp_replace(col("Annual_Base_Salary"), "\£",
                                                                                  "")).withColumn("Signing_Bonus",
                                                                                                  regexp_replace(
                                                                                                      col("Signing_Bonus"),
                                                                                                      "\$",
                                                                                                      "")).withColumn(
    "Signing_Bonus",
    regexp_replace(col("Signing_Bonus"), ",", "")).withColumn("Annual_Bonus",
                                                              regexp_replace(col("Annual_Bonus"), "\£",
                                                                             "")).withColumn("Annual_Bonus",
                                                                                             regexp_replace(
                                                                                                 col("Annual_Bonus"),
                                                                                                 "\$",
                                                                                                 "")).withColumn(
    "Annual_Bonus",
    regexp_replace(col("Annual_Bonus"), ",", "")).withColumn("Annual_Bonus",
                                                             regexp_replace(col("Annual_Bonus"), "\£", ""))
exclude_corrupt_salary_records = clean_amount_columns.filter(
    ~col("Annual_Base_Salary").rlike("[^0-9]"))  # Filtering out bad records while retaining good records.
include_corrupt_salary_records = clean_amount_columns.filter(col("Annual_Base_Salary").rlike(
    "[^0-9]"))  # Retaining bad records for Auditing. We can write these records to some location so that source people can fix this records or send out some kind of DD or Mapping Values using which we can standardize this column values.
print(include_corrupt_salary_records.count())
updated_gender = exclude_corrupt_salary_records.withColumn("Gender",
                                                           when(((lower(col("Gender")) != "male") & (
                                                                   lower(col("Gender")) != "female")),
                                                                lit("other")).otherwise(
                                                               lower(
                                                                   trim(
                                                                       col("Gender")))))  # Have transformed the records for people who added comments in the Gender section as "other". Also added case consistency throughout the column values.

updated_work_exp = updated_gender.withColumn("Work_Exp",
                                             split(col("Work_Exp"), " ")[
                                                 0])  # Extracting the data based on space. Basically replacing value-with-comments records with only values.
updated_work_exp_final = updated_work_exp.withColumn("Work_Exp", translate(col("Work_Exp"), "-< ",
                                                                           ""))  # Removing unwanted characters from the data.

updated_company_name_final = updated_work_exp_final.withColumn("Company_Name", upper(col("Company_Name")))

updated_location = updated_company_name_final.withColumn("Location",
                                                         lower(translate(col("Location"), ", ", "_"))).withColumn(
    "Location",
    regexp_replace(col("Location"), ";", "_"))  # Cleaning the location column along with maintaining consistency.
updated_location_final = updated_location.withColumn("Location",
                                                     when(col("Location").rlike("[^0-9]"), col("Location")).otherwise(
                                                         lit("")))  # Replacing numeric characters under Location column with a blank since we don't having mapping what those numeric values could be

updated_job_title = updated_location_final.withColumn("Job_Title",
                                                      upper(col("Job_Title")))  # Capitalising values of this column.

updated_annual_bonus = updated_job_title.filter(
    ~col("Annual_Bonus").rlike("[^0-9]"))  # Retaining numeric values ONLY for annual bonus.\
corrupt_annual_bonus = updated_job_title.filter(
    col("Annual_Bonus").rlike("[^0-9]"))

# corrupt_annual_bonus.show(100, truncate=False)
corrupt_annual_bonus_records_along_with_salary = include_corrupt_salary_records.unionAll(
    corrupt_annual_bonus)  # Appending bad records

updated_signing_bonus = updated_annual_bonus.filter(
    ~col("Signing_Bonus").rlike("[^0-9]"))  # Retaining numeric values ONLY for signing_bonus.
corrupt_signing_bonus_records = updated_annual_bonus.filter(
    col("Signing_Bonus").rlike("[^0-9]"))  # Separating bad records.
# corrupt_signing_bonus_records.show(100, truncate=False)
corrupt_signing_bonus_records_along_with_salary_and_annual = corrupt_annual_bonus_records_along_with_salary.unionAll(
    corrupt_signing_bonus_records)  # Appending final bad records for auditing..

corrupt_signing_bonus_records_along_with_salary_and_annual.show(100, truncate=False)
print(corrupt_signing_bonus_records_along_with_salary_and_annual.count())
print(updated_signing_bonus.count())
updated_signing_bonus.show(400, truncate=False)
