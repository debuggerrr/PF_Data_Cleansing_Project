
from pyspark.sql.functions import *


class Data_1():


    def Start(self, spark, path_data_1):
        df = spark.read.option("header", "true").option("multiline", "true").option("escape", "\"").csv(
            path_data_1).selectExpr("Timestamp", "`How old are you?`",
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


        # trimmed_data.filter(col("Created_At") == '4/27/2019 1:02:05').select("*").show(truncate=False)
        updated_for_null = trimmed_data.withColumn("Created_At",
                                                   when(col("Created_At").isNull(),
                                                        lit("01/01/1900 00:00:00")).otherwise(
                                                       col("Created_At"))).withColumn(
            "Job_Title",
            when(col("Job_Title").isNull(), lit("")).otherwise(
                col("Job_Title"))).withColumn("Annual_Salary",
                                              when(col("Annual_Salary").isNull(), lit("0")).otherwise(
                                                  col("Annual_Salary"))).withColumn("Industry",
                                                                                    when(col("Industry").isNull(),
                                                                                         lit("")).otherwise(
                                                                                        col("Industry"))).withColumn(
            "Location",
            when(col("Location").isNull(), lit("")).otherwise(
                col("Location"))).withColumn("Job_Title",
                                             when(col("Job_Title").isNull(), lit("")).otherwise(
                                                 col("Job_Title"))).withColumn("Age_Range",
                                                                               when(col("Age_Range").isNull(),
                                                                                    lit("0")).otherwise(
                                                                                   col("Age_Range"))).withColumn(
            "Work_Experience",
            when(col("Work_Experience").isNull(), lit("0")).otherwise(
                col("Work_Experience")))  # Handling NULL cases with default values.

        exclude_corrupt_salary_records = updated_for_null.filter(
            (~col("Annual_Salary").rlike('[A-Za-z]')))  # Filtering out bad records while retaining good records.
        include_corrupt_salary_records = updated_for_null.filter(col("Annual_Salary").rlike(
            '[A-Za-z]'))  # Retaining bad records for Auditing. We can write these records to some location so that source people can fix this records or send out some kind of DD or Mapping Values using which we can standardize this column values.

        # include_corrupt_salary_records.show(50, truncate=False)

        annual_salary_clean_up = exclude_corrupt_salary_records.withColumn("Annual_Salary",
                                                                           split(col("Annual_Salary"), "\(")[
                                                                               0]).withColumn(
            "Annual_Salary",
            regexp_replace(col("Annual_Salary"), "\$",
                           "")).withColumn("Annual_Salary",
                                           regexp_replace(
                                               col("Annual_Salary"),
                                               ", ",
                                               "")).withColumn(
            "Annual_Salary",
            regexp_replace(col("Annual_Salary"), "£",
                           "")).withColumn("Annual_Salary",
                                           regexp_replace(col("Annual_Salary"), "yr",
                                                          "")).withColumn("Annual_Salary",
                                                                          regexp_replace(col("Annual_Salary"), "~",
                                                                                         "")).withColumn(
            "Annual_Salary",
            regexp_replace(
                col("Annual_Salary"),
                ",",
                "")).withColumn(
            "Annual_Salary",
            regexp_replace(
                col("Annual_Salary"),
                " ",
                ""))  # $, £yr~ Performing some basic checks in order to remove unwanted characters.
        # annual_salary_clean_up.filter(col("Job_Title") == 'Principal Mechanical Systems Engineer').show(truncate=False)

        updated_industry = annual_salary_clean_up.withColumn("Industry", upper(
            col("Industry")))  # Maintaining consistency through the data by capitalizing records.

        updated_job_title = updated_industry.withColumn("Job_Title", upper(
            col("Job_Title")))  # Maintaining consistency through the data by capitalizing records.

        segregation_of_columns_df = updated_job_title.withColumn("Min_Age", split(col("Age_Range"), "-")[0]).withColumn(
            "Max_Age", split(col("Age_Range"), "-")[1]).drop(
            "Age_Range")  # Segregation of Age Range into Min and Max Age in order to make the further computations easier

        segregate_work_experience = segregation_of_columns_df.withColumn("Work_Experience",
                                                                         split(regexp_replace("Work_Experience", " - ",
                                                                                              "-"),
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

        segregate_work_experience_final = segregate_work_experience_temp_final.drop("Max_Exp").withColumnRenamed(
            "Max_Exp_New",
            "Max_Exp")  # Substituting the existing Max_Exp column with newly calculated Max_Exp column and renaming it.

        city_state_country = segregate_work_experience_final.withColumn("Location",
                                                                        regexp_replace(col("Location"), " ",
                                                                                       "")).withColumn(
            "Location",
            regexp_replace(lower(col("Location")), ", ",
                           "_")).withColumn("Location", regexp_replace(
            col("Location"), "/", "_")).withColumn("Location", regexp_replace(col("Location"), ",",
                                                                              "")).withColumn("Location",
                                                                                              split(col("Location"),
                                                                                                    "\(")[
                                                                                                  0])  # Modifying the location data in order to maintain data consistency for this column.

        city_state_country.createOrReplaceTempView("before_final")
        # city_state_country.show(500, truncate=False)
        final_cleaned_data_1 = spark.sql("""select to_timestamp(Created_At,'M/d/yyyy H:mm:ss') as Created_At,Industry,CAST(Job_Title as string) as Job_Title, CAST(Annual_Salary as decimal(38,2)) as Annual_Salary, CAST(Location as string) as Location, CAST(Min_Age as integer) as Min_Age,
        CAST(Max_Age as integer) as Max_Age, CAST(Min_Exp as float) as Min_Exp, CAST(Max_Exp as float) as Max_Exp from before_final
        """)  # Casting the columns as per their probably expected data types

        # final_cleaned_data_1.filter(col("Created_At") == '2019-04-25 00:03:06').show(400, truncate=False)
        # final_cleaned_data_1.show(400, truncate=False)
        return ("Success", include_corrupt_salary_records, final_cleaned_data_1)
