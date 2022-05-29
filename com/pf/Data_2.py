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
    "/home/lumiq/Documents/salary_data/data_2.csv")  # No use of infering the schema since spark won't be able to identify since the records are not clean so by default it will consider all the columns of string type ONLY.

df.printSchema()

new_columns_list = ["Created_At", "Employment_Type", "Company_Name", "Company_Size", "Country", "City",
                    "Industry", "Public_Private_Company", "Work_Experience", "Experience_With_Current_Company",
                    "Job_Title", "Job_Ladder", "Job_Level", "Hours_Per_Week", "Actual_Hours_Per_Week",
                    "Highest_Formal_Eductaion",
                    "Base_Salary_2018", "Bonus_2018", "Stock_Equity_Value", "Health_Insurance",
                    "Annual_Vacation_In_Weeks", "Happy_With_Job", "Want_To_Resign", "Thoughts", "Gender",
                    "Booming_Skills", "Attended_Bootcamp"]
renamed_df = df.toDF(*new_columns_list)

trimmed_data = ""
for i in renamed_df.columns:
    trimmed_data = renamed_df.withColumn(i, trim(
        col(i)))  # Removing of leading and trailing spaces from the dataset values.
trimmed_data.printSchema()

print(trimmed_data.count())
trimmed_data.show(100, truncate=False)
