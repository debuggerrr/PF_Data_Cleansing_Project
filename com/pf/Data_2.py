import io
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


class Data_2():

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def Start(self, spark, path_data_2):
        df = spark.read.option("header", "true").option("multiline", "true").option("escape", "\"").csv(
            path_data_2).selectExpr("Timestamp", "`Company Name`",
                                    "`Primary Location (Country)`",
                                    "`Primary Location (City)`",
                                    "`Industry in company`",
                                    "`Years Experience in Industry`",
                                    "`Job Title In Company`",
                                    "`Required Hours Per Week`", "`Actual Hours Per Week`",
                                    "`Total Base Salary in 2018 (in USD)`",
                                    "`Total Bonus in 2018 (cumulative annual value in USD)`",
                                    "`Annual Vacation (in Weeks)`",
                                    "`Are you happy at your current position?`",
                                    "`Do you plan to resign in the next 12 months?`",
                                    "Gender")  # No use of infering the schema since spark won't be able to identify since the records are not clean so by default it will consider all the columns of string type ONLY.

        df.printSchema()

        new_columns_list = ["Created_At", "Company_Name", "Country", "City",
                            "Industry", "Work_Exp",
                            "Job_Title", "Hours_Per_Week", "Actual_Hours_Per_Week",
                            "Base_Salary_2018", "Bonus_2018",
                            "Annual_Vacation_In_Weeks", "Happy_With_Job", "Want_To_Resign", "Gender"]
        renamed_df = df.toDF(*new_columns_list)

        trimmed_data = ""
        for i in renamed_df.columns:
            trimmed_data = renamed_df.withColumn(i, trim(
                col(i)))  # Removing of leading and trailing spaces from the dataset values.
        trimmed_data.printSchema()

        updated_for_null = trimmed_data.withColumn("Created_At",
                                                   when(col("Created_At").isNull(),
                                                        lit("01/01/1900 00:00:00")).otherwise(
                                                       col("Created_At"))).withColumn("Company_Name",
                                                                                      when(col("Company_Name").isNull(),
                                                                                           lit("")).otherwise(
                                                                                          col("Company_Name"))).withColumn(
            "Country",
            when(col("Country").isNull(), lit("")).otherwise(
                col("Country"))).withColumn("City",
                                            when(col("City").isNull(), lit("")).otherwise(
                                                col("City"))).withColumn("Industry",
                                                                         when(col("Industry").isNull(),
                                                                              lit("")).otherwise(
                                                                             col("Industry"))).withColumn(
            "Work_Exp",
            when(col("Work_Exp").isNull(), lit("0")).otherwise(
                col("Work_Exp"))).withColumn("Job_Title",
                                             when(col("Job_Title").isNull(), lit("")).otherwise(
                                                 col("Job_Title"))).withColumn("Hours_Per_Week",
                                                                               when(col("Hours_Per_Week").isNull(),
                                                                                    lit("0")).otherwise(
                                                                                   col("Hours_Per_Week"))).withColumn(
            "Actual_Hours_Per_Week",
            when(col("Actual_Hours_Per_Week").isNull(), lit("0")).otherwise(
                col("Actual_Hours_Per_Week"))).withColumn("Base_Salary_2018",
                                                          when(col("Base_Salary_2018").isNull(), lit("0")).otherwise(
                                                              col("Base_Salary_2018"))).withColumn("Bonus_2018",
                                                                                                   when(
                                                                                                       col("Bonus_2018").isNull(),
                                                                                                       lit("0")).otherwise(
                                                                                                       col("Bonus_2018"))).withColumn(
            "Annual_Vacation_In_Weeks",
            when(col("Annual_Vacation_In_Weeks").isNull(), lit("0")).otherwise(
                col("Annual_Vacation_In_Weeks"))).withColumn("Happy_With_Job",
                                                             when(col("Happy_With_Job").isNull(), lit("")).otherwise(
                                                                 col("Happy_With_Job"))).withColumn("Want_To_Resign",
                                                                                                    when(
                                                                                                        col("Want_To_Resign").isNull(),
                                                                                                        lit("")).otherwise(
                                                                                                        col("Want_To_Resign"))).withColumn(
            "Gender",
            when(col("Gender").isNull(), lit("other")).otherwise(
                col("Gender")))  # Handling NULL cases with default values.

        clean_base_salary = updated_for_null.filter(
            ~col("Base_Salary_2018").rlike('[A-Za-z]'))  # Filtering only numeric records.

        clean_bonus = clean_base_salary.filter(~col("Bonus_2018").rlike('[A-Za-z]'))  # Filtering only numeric records.

        corrupt_salary_records = updated_for_null.filter(
            col("Base_Salary_2018").rlike('[A-Za-z]'))

        corrupt_bonus_records = clean_base_salary.filter(col("Bonus_2018").rlike('[A-Za-z]'))

        total_corrupt_records = corrupt_salary_records.unionAll(corrupt_bonus_records)

        clean_company = clean_bonus.withColumn("Company_Name", upper(
            col("Company_Name")))  # Maintaining consistency through out the records.

        concat_location = clean_company.withColumn("Country", when(((col("City") == "") & (col("Country") == "")),
                                                                   lit("")).when(
            (col("City") == "") & (~col("Country").isNull()),
            lower(col("Country"))).otherwise(
            lower(concat(col("City"), lit("_"), col("Country"))))).drop(
            "City").withColumnRenamed("Country", "Location").withColumn("Location",
                                                                        regexp_replace(col("Location"), " ",
                                                                                       "")).withColumn(
            "Location", regexp_replace(col("Location")
                                       , ", ", "")).withColumn("Location", split(col("Location"), "\(")[
            0]).withColumn("Location", regexp_replace(col("Location"), ",",
                                                      ""))  # Merging City and Country as Location and cleaning the data and maintaining consistency.

        clean_industry = concat_location.withColumn("Industry", upper(col("Industry"))).withColumn("Industry",
                                                                                                   regexp_replace(
                                                                                                       col("Industry"),
                                                                                                       " - ",
                                                                                                       "-")).withColumn(
            "Industry", regexp_replace(col("Industry"), "/",
                                       "-"))  # Cleaning Industry column and maintaining consistency throughout the records.

        clean_work_exp = clean_industry.withColumn("Work_Exp", regexp_replace(col("Work_Exp"), " ", "")).withColumn(
            "Work_Exp",
            regexp_replace(
                col("Work_Exp"),
                "\+",
                "")).withColumn(
            "Min_Exp", split(col("Work_Exp"), "-")[0]).withColumn("Max_Exp", split(col("Work_Exp"), "-")[1]).drop(
            "Work_Exp").withColumn("Max_Exp", when(col("Max_Exp").isNull(), col("Min_Exp")).otherwise(
            col("Max_Exp")))  # Cleaning Work Experience and splitting it in two sections as Min and Max Exp. Also, assuming 40+ as min 40 and max 40 as experience.
        clean_hours_per_week = clean_work_exp.withColumn("Hours_Per_Week",
                                                         regexp_replace(col("Hours_Per_Week"), " ", "")).withColumn(
            "Hours_Per_Week",
            regexp_replace(
                col("Hours_Per_Week"),
                "\+",
                "")).withColumn(
            "Min_Hours_Per_Week", split(col("Hours_Per_Week"), "-")[0]).withColumn("Max_Hours_Per_Week",
                                                                                   split(col("Hours_Per_Week"), "-")[
                                                                                       1]).drop(
            "Hours_Per_Week").withColumn("Max_Hours_Per_Week",
                                         when(col("Max_Hours_Per_Week").isNull(), col("Min_Hours_Per_Week")).otherwise(
                                             col("Max_Hours_Per_Week")))  # Cleaning Hours Per Week and splitting it in two sections as Min and Max Exp. Also, assuming 40+ as min 40 and max 40 as experience.
        clean_job_title = clean_hours_per_week.withColumn("Job_Title", upper(
            col("Job_Title")))  # Maintaining consistency through out the Job_Title Column.
        clean_actual_hours_per_week = clean_job_title.withColumn("Actual_Hours_Per_Week",
                                                                 regexp_replace(col("Actual_Hours_Per_Week"), " ",
                                                                                "")).withColumn(
            "Actual_Hours_Per_Week",
            regexp_replace(
                col("Actual_Hours_Per_Week"),
                "\+",
                "")).withColumn(
            "Min_Actual_Hours_Per_Week", split(col("Actual_Hours_Per_Week"), "-")[0]).withColumn(
            "Max_Actual_Hours_Per_Week",
            split(
                col("Actual_Hours_Per_Week"),
                "-")[1]).drop(
            "Actual_Hours_Per_Week").withColumn("Max_Actual_Hours_Per_Week",
                                                when(col("Max_Actual_Hours_Per_Week").isNull(),
                                                     col("Min_Actual_Hours_Per_Week")).otherwise(
                                                    col("Max_Actual_Hours_Per_Week")))  # Cleaning Hours Per Week and splitting it in two sections as Min and Max Hours. Also, assuming 40+ as min 40 and max 40 as Actual hours per week.

        clean_annual_vacation = clean_actual_hours_per_week.withColumn("Annual_Vacation_In_Weeks",
                                                                       regexp_replace(col("Annual_Vacation_In_Weeks"),
                                                                                      "\+",
                                                                                      ""))  # Cleaning data for this column...

        clean_happy_with_job = clean_annual_vacation.withColumn("Happy_With_Job",
                                                                upper(col("Happy_With_Job")))  # Capitalising values...

        clean_want_to_resign = clean_happy_with_job.withColumn("Want_To_Resign",
                                                               upper(col("Want_To_Resign")))  # Capitalising values...

        clean_gender = clean_want_to_resign.withColumn("Gender", upper(col("Gender")))  # Capitalising values...

        clean_gender.createOrReplaceTempView("data2")
        data_2_final = spark.sql(
            """select to_timestamp(Created_At,'M/d/yyyy H:mm:ss') as Created_At,Company_Name,Location,Industry,Job_Title,
            CAST(Base_Salary_2018 as Decimal(38,2)) as Base_Salary_2018,CAST(Bonus_2018 as Decimal(38,2)) as Bonus_2018, CAST(Annual_Vacation_In_Weeks as integer) as Annual_Vacation_In_Weeks,
            Happy_With_Job,Want_To_Resign,Gender,CAST(Min_Exp as float) as Min_Exp,CAST(Max_Exp as float) as Max_Exp,CAST(Min_Hours_Per_Week as integer) as Min_Hours_Per_Week, CAST(Max_Hours_Per_Week as integer) as Max_Hours_Per_Week,
            CAST(Min_Actual_Hours_Per_Week as integer) as Min_Actual_Hours_Per_Week,CAST(Max_Actual_Hours_Per_Week as integer) as Max_Actual_Hours_Per_Week from data2
            """)  # Casting of columns as per their expected data types.

        return ("Success", data_2_final,total_corrupt_records)
