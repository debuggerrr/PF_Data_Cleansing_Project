
from pyspark.sql.functions import *

class Data_3():


    def Start(self, spark, path_data_3):
        df = spark.read.option("header", "true").option("multiline", "true").option("escape", "\"").csv(
            "/home/lumiq/Documents/salary_data/data_3.csv").selectExpr("Timestamp", "Employer", "Location",
                                                                       "`Job Title`",
                                                                       "`Years of Experience`",
                                                                       "`Annual Base Pay`", "`Signing Bonus`",
                                                                       "`Annual Bonus`",
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
                                                   when(col("Created_At").isNull(),
                                                        lit("01/01/1900 00:00:00")).otherwise(
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
                                                                                        when(
                                                                                            col("Signing_Bonus").isNull(),
                                                                                            lit("0")).otherwise(
                                                                                            col("Signing_Bonus"))).withColumn(
            "Annual_Bonus",
            when(col("Annual_Bonus").isNull(), lit("0")).otherwise(
                col("Annual_Bonus"))).withColumn("Gender",
                                                 when(col("Gender").isNull(), lit("")).otherwise(
                                                     col("Gender")))  # Handling NULL cases with default values.
        exclude_corrupt_salary_records = updated_for_null.filter(
            ~col("Annual_Base_Salary").rlike('[A-Za-z]'))

        include_corrupt_salary_records = updated_for_null.filter(col("Annual_Base_Salary").rlike(
            '[A-Za-z]'))  # Retaining bad records for Auditing. We can write these records to some location so that source people can fix this records or send out some kind of DD or Mapping Values using which we can standardize this column values.

        updated_signing_bonus = exclude_corrupt_salary_records.filter(
            ~col("Signing_Bonus").rlike('[A-Za-z]'))  # Retaining numeric values ONLY for signing_bonus.

        corrupt_signing_bonus_records = exclude_corrupt_salary_records.filter(
            col("Signing_Bonus").rlike('[A-Za-z]'))  # Separating bad records.

        updated_annual_bonus = updated_signing_bonus.filter(
            (~col("Annual_Bonus").rlike('[A-Za-z]')) & (~col("Annual_Bonus").rlike("\-")) & (
                ~col("Annual_Bonus").rlike("\%")))  # Retaining numeric values ONLY for annual bonus.

        corrupt_annual_bonus = updated_signing_bonus.filter(
            (col("Annual_Bonus").rlike(
                '[A-Za-z]')) & (col("Annual_Bonus").rlike("\-")) & (
                col("Annual_Bonus").rlike(
                    "\%")))  # Retaining bad records for Auditing. We can write these records to some location so that source people can fix this records or send out some kind of DD or Mapping Values using which we can standardize this column values.

        total_corrupt_records = include_corrupt_salary_records.unionAll(
            corrupt_annual_bonus).unionAll(corrupt_signing_bonus_records)  # Appending all the bad records for auditing

        #total_corrupt_records.show(100, truncate=False)

        clean_amount_columns = updated_annual_bonus.withColumn("Annual_Base_Salary",
                                                               regexp_replace(col("Annual_Base_Salary"), "\$",
                                                                              "")).withColumn(
            "Annual_Base_Salary",
            regexp_replace(col("Annual_Base_Salary"), "\€", "")).withColumn(
            "Annual_Base_Salary",
            regexp_replace(col("Annual_Base_Salary"), ",", "")).withColumn("Annual_Base_Salary",
                                                                           regexp_replace(col("Annual_Base_Salary"),
                                                                                          "\£",
                                                                                          "")).withColumn(
            "Annual_Base_Salary",
            regexp_replace(
                col("Annual_Base_Salary"),
                " ",
                "")).withColumn(
            "Annual_Base_Salary",
            split(col("Annual_Base_Salary"), " USD")[0]).withColumn("Signing_Bonus",
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
        print(include_corrupt_salary_records.count())
        updated_gender = clean_amount_columns.withColumn("Gender",
                                                         when(((lower(col("Gender")) != "male") & (
                                                                 lower(col("Gender")) != "female")),
                                                              lit("OTHER")).otherwise(
                                                             upper(
                                                                 trim(
                                                                     col("Gender")))))  # Have transformed the records for people who added comments in the Gender section as "other". Also added case consistency throughout the column values.

        updated_work_exp = updated_gender.withColumn("Work_Exp",
                                                     split(col("Work_Exp"), " ")[
                                                         0])  # Extracting the data based on space. Basically replacing value-with-comments records with only values.
        updated_work_exp_final = updated_work_exp.withColumn("Work_Exp", translate(col("Work_Exp"), "-< ",
                                                                                   ""))  # Removing unwanted characters from the data.

        updated_company_name_final = updated_work_exp_final.withColumn("Company_Name", upper(col("Company_Name")))

        updated_location = updated_company_name_final.withColumn("Location",
                                                                 lower(
                                                                     translate(col("Location"), ", ", "_"))).withColumn(
            "Location",
            regexp_replace(col("Location"), ";",
                           "_"))  # Cleaning the location column along with maintaining consistency.
        updated_location_final = updated_location.withColumn("Location",
                                                             when(col("Location").rlike("[^0-9]"),
                                                                  col("Location")).otherwise(
                                                                 lit("")))  # Replacing numeric characters under Location column with a blank since we don't having mapping what those numeric values could be

        updated_job_title = updated_location_final.withColumn("Job_Title",
                                                              upper(
                                                                  col("Job_Title")))  # Capitalising values of this column.

        # corrupt_annual_bonus.show(100, truncate=False)

        updated_job_title.createOrReplaceTempView("data3")

        final_data = spark.sql("""
        select to_timestamp(Created_At,'M/d/yyyy H:mm:ss') as Created_At,Company_Name,Location,Job_Title,CAST(Work_Exp as float) as Work_Exp, CAST(Annual_Base_Salary as decimal(38,2)) as Annual_Base_Salary,
        CAST(Signing_Bonus as decimal(38,2)) as Signing_Bonus, CAST(Annual_Bonus as decimal(38,2)) as Annual_Bonus,Gender
        from data3
        """)  # Casting data at the end as per their respective data types...
        return ("Success", final_data, total_corrupt_records)
        # final_data.printSchema()
        # final_data.show(400, truncate=False)
