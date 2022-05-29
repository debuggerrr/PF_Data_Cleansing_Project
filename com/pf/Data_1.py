import io
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.memory", "8g") \
    .appName('my-cool-app') \
    .getOrCreate()

df = spark.read.option("header", "true").option("multiline", "true").option("escape", "\"").csv(
    "/home/lumiq/Documents/salary_data/data_1.csv").selectExpr("Timestamp", "`How old are you?`",
                                                               "`What industry do you work in?`",
                                                               "`Job title`", "`What is your annual salary?`",
                                                               "`Where are you located? (City/state/country)`",
                                                               "`How many years of post-college professional work experience do you have?`",
                                                               )  # Selecting subset of columns ONLY as per the required problem statement.

df.printSchema()

new_columns_list = ["Created_At", "Age_Range", "Industry", "Job_Title", "Annual_Salary",
                    "Location", "Work_Experience"]
renamed_df = df.toDF(*new_columns_list)

trimmed_data = ""
for i in renamed_df.columns:
    trimmed_data = renamed_df.withColumn(i, trim(
        col(i)))  # Removing of leading and trailing spaces from the dataset values.

# Casting of columns at the end.
# Remove double quotes from the column values -- Done
# Remove dollar / other currency signs and comma, space from the salary column.
# Convert the salary column to double/long format.
# Remove leading / Trailing spaces from the column values. -- Done
# Add consistency to the Location column
# Split up the Age and Experience columns in order to make the further aggregation computations easier. Make assumptions for null values if any -- Done
# Do casting of columns at the end because if the casting is successful then it would mean that the column records have been cleaned as per their expected data types or else it will throw an error if some value is not able to convert it to the asked data type.

# NULL VALUES: In the actual scenario would have confirmed from the client what to do with these "null" values. Whether they want it to be replaced/eliminate/ keep some default values for it.


# trimmed_data.filter(col("Created_At") == '4/27/2019 1:02:05').select("*").show(truncate=False)

annual_salary_clean_up = trimmed_data.withColumn("Annual_Salary",
                                                 regexp_replace(col("Annual_Salary"), "\$",
                                                                "")).withColumn("Annual_Salary",
                                                                                regexp_replace(col("Annual_Salary"),
                                                                                               ", ",
                                                                                               "")).withColumn(
    "Annual_Salary",
    regexp_replace(col("Annual_Salary"), "£",
                   "")).withColumn("Annual_Salary",
                                   regexp_replace(col("Annual_Salary"), "yr",
                                                  "")).withColumn("Annual_Salary",
                                                                  regexp_replace(col("Annual_Salary"), "~",
                                                                                 "")).withColumn("Annual_Salary",
                                                                                                 regexp_replace(
                                                                                                     col("Annual_Salary"),
                                                                                                     ",",
                                                                                                     "")).withColumn(
    "Annual_Salary",
    regexp_replace(
        col("Annual_Salary"),
        " ",
        "")).withColumn("Annual_Salary",
                        regexp_replace(
                            col("Annual_Salary"),
                            "\.00",
                            ""))  # $, £yr~ Performing some basic checks in order to remove unwanted characters.
# annual_salary_clean_up.filter(col("Job_Title") == 'Principal Mechanical Systems Engineer').show(truncate=False)
exclude_corrupt_salary_records = annual_salary_clean_up.filter(
    (~col("Annual_Salary").rlike("[^0-9]")))  # Filtering out bad records while retaining good records.
include_corrupt_salary_records = annual_salary_clean_up.filter(col("Annual_Salary").rlike(
    "[^0-9]"))  # Retaining bad records for Auditing. We can write these records to some location so that source people can fix this records or send out some kind of DD or Mapping Values using which we can standardize this column values.
include_corrupt_salary_records.filter(include_corrupt_salary_records.Created_At == '4/24/2019 11:49:34').show(
    truncate=False)
include_corrupt_salary_records.show(50, truncate=False)
updated_industry = exclude_corrupt_salary_records.withColumn("Industry", upper(
    col("Industry")))  # Maintaining consistency through the data by capitalizing records.

updated_job_title = updated_industry.withColumn("Job_Title", upper(
    col("Job_Title")))  # Maintaining consistency through the data by capitalizing records.

segregation_of_columns_df = updated_job_title.withColumn("Min_Age", split(col("Age_Range"), "-")[0]).withColumn(
    "Max_Age", split(col("Age_Range"), "-")[1]).drop(
    "Age_Range")  # Segregation of Age Range into Min and Max Age in order to make the further computations easier

segregate_work_experience = segregation_of_columns_df.withColumn("Work_Experience",
                                                                 split(regexp_replace("Work_Experience", " - ", "-"),
                                                                       " y")[
                                                                     0])  # Cleaning the Work_Experience column by removing the extra whitespaces in between.
segregate_work_experience_in_min_max = segregate_work_experience.withColumn("Min_Exp",
                                                                            split(col("Work_Experience"), "-")[
                                                                                0]).withColumn("Max_Exp", split(
    col("Work_Experience"), "-")[1]).drop(
    "Work_Experience")  # Splitting and segregating the data for min and max experience in order to make the further computations easier.
segregate_work_experience_in_min_max.createOrReplaceTempView("experience")

segregate_work_experience_temp_final = spark.sql(
    "select *, case when Max_Exp is null then Min_Exp else Max_Exp end as Max_Exp_New from experience")  # Adding a check in order to eliminate null values from Max_Exp column.

segregate_work_experience_final = segregate_work_experience_temp_final.drop("Max_Exp").withColumnRenamed("Max_Exp_New",
                                                                                                         "Max_Exp")  # Substituting the existing Max_Exp column with newly calculated Max_Exp column and renaming it.

city_state_country = segregate_work_experience_final.withColumn("Location",
                                                                translate(lower(col("Location")), ", ",
                                                                          "_")).withColumn("Location", regexp_replace(
    col("Location"), "/", "_"))  # Modifying the location data in order to maintain data consistency for this column.

city_state_country_final = city_state_country.withColumn("Location", split(col("Location"), ' ')[0]).withColumn(
    "Location", split(col("Location"), "\(")[0])

city_state_country_final.createOrReplaceTempView("before_final")
city_state_country_final.show(500, truncate=False)
final_cleaned_data_1 = spark.sql("""select to_timestamp(Created_At,'M/dd/yyyy H:mm:ss') as Created_At,CAST(Job_Title as string) as Job_Title, round(CAST(Annual_Salary as double),2) as Annual_Salary, CAST(Location as string) as Location, CAST(Min_Age as integer) as Min_Age,
CAST(Max_Age as integer) as Max_Age, CAST(Min_Exp as integer) as Min_Exp, CAST(Max_Exp as integer) as Max_Exp from before_final
""")  # Casting the columns as per their probably expected data types

# final_cleaned_data_1.orderBy(col("Created_At").desc()).show(400, truncate=False)
# final_cleaned_data_1.filter(col("Created_At") == '2019-05-01 0:02:19').show(400, truncate=False)
final_cleaned_data_1.filter(col("Created_At") == '2019-04-25 00:03:06').show(400, truncate=False)
final_cleaned_data_1.show(400, truncate=False)
final_cleaned_data_1.printSchema()
