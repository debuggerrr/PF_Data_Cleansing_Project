
from functools import reduce


class Aggregations():

    def start_aggregations(self, spark, data1DF, data2DF, data3DF):
        data1DF.printSchema()
        data2DF.printSchema()
        data3DF.printSchema()

        data1DF.createOrReplaceTempView("data_1")
        data2DF.createOrReplaceTempView("data_2")
        data3DF.createOrReplaceTempView("data_3")

        data1_modified = spark.sql("""
        select Created_At,'' as Company_Name,Location,Industry,Job_Title,Annual_Salary as Salary,CAST('0' as decimal(38,2)) as Bonus,
         CAST('0' as integer) as Annual_Vacation_In_Weeks,'' as Happy_With_Job, '' as Want_To_Resign, 
         'other' as Gender,Min_Exp,Max_Exp, CAST('0' as float) as Min_Hours_Per_Week,CAST('0' as float) as Max_Hours_Per_Week,
         CAST('0' as float) as Min_Actual_Hours_Per_Week,CAST('0' as float) as Max_Actual_Hours_Per_Week from data_1
         """)  # Creating a schema in order to merge the 3 datasets...

        data2_modified = spark.sql("""
        select Created_At,Company_Name,Location,Industry,Job_Title,Base_Salary_2018 as Salary,Bonus_2018 as Bonus,
         Annual_Vacation_In_Weeks,Happy_With_Job,Want_To_Resign, 
         Gender,Min_Exp,Max_Exp, CAST(Min_Hours_Per_Week as float) as Min_Hours_Per_Week,CAST(Max_Hours_Per_Week as float) as Max_Hours_Per_Week,
         CAST(Min_Actual_Hours_Per_Week as float) as Min_Actual_Hours_Per_Week,
         CAST(Max_Actual_Hours_Per_Week as float) as Max_Actual_Hours_Per_Week from data_2
         """)  # Creating a schema in order to merge the 3 datasets...

        data3_modified = spark.sql("""
        select Created_At,Company_Name,Location,'' as Industry,Job_Title,Annual_Base_Salary as Salary,Annual_Bonus as Bonus,
         CAST('0' as integer) as Annual_Vacation_In_Weeks,'' as Happy_With_Job, '' as Want_To_Resign, 
         Gender,CAST('0' as float) as Min_Exp,Work_Exp as Max_Exp, CAST('0' as float) as Min_Hours_Per_Week,CAST('0' as float) as Max_Hours_Per_Week,
         CAST('0' as float) as Min_Actual_Hours_Per_Week,CAST('0' as float) as Max_Actual_Hours_Per_Week from data_3
         """)  # Creating a schema in order to merge the 3 datasets...

        final_dataset = data1_modified.unionAll(data2_modified).unionAll(
            data3_modified)  # Merging all the rows from all the 3 datasets...

        ###########Aggregations################################

        '''
        FIRST PROBLEM:
        '''
        # Assuming role to be Job_Title

        roles_list = ["SOFTWARE", "DATA", "TECH", "ENGINEERING", "WEB", "DEVELOPER", "UX", "Project Manager",
                      "FULL STACK", "ARCHITECT"]

        df1 = final_dataset.where(
            reduce(lambda a, b: a | b, (final_dataset['Job_Title'].like('%' + pat + "%") for pat in roles_list))
        )

        df1.createOrReplaceTempView("first_problem")

        final_first_problem = spark.sql(
            "SELECT Job_Title, round(AVG(Salary),2) as Compensation from first_problem group by Job_Title")

        final_first_problem.show(100, truncate=False)
        final_first_problem.repartition(1).write.mode("overwrite").option("header", "true").csv(
            "/home/lumiq/Documents/test_data/first_problem/")

        '''
        SECOND PROBLEM:
        '''

        final_dataset.createOrReplaceTempView("for_all_problems")

        second_problem = spark.sql("""SELECT Location,round(AVG(Salary),2) as Average_Compensation, round(MIN(Salary),2) as Min_Compensation,
                                   round(MAX(Salary),2) as Max_Compensation from for_all_problems group by Location
                                   """)
        second_problem.repartition(1).write.mode("overwrite").option("header", "true").csv(
            "/home/lumiq/Documents/test_data/second_problem/")
        second_problem.show(100, truncate=False)

        '''
        THIRD PROBLEM
        '''

        third_problem = spark.sql(
            "SELECT Salary as Cash_Compensation, Bonus as Other_Forms_Of_Compensation from for_all_problems ")
        third_problem.repartition(1).write.mode("overwrite").option("header", "true").csv(
            "/home/lumiq/Documents/test_data/third_problem/")

        '''
        FOURTH PROBLEM: List top 5 industries under which people are much happier.
        '''

        fourth_problem = spark.sql("""
        with yes_only as
        (
        select Industry,Happy_With_Job from for_all_problems where Happy_With_Job='YES'
        ),
        top_5_industry as
        (
        select Industry, count(Happy_With_Job) as happy_counts from yes_only group by Industry order by count(Happy_With_Job) desc
        )
        select Industry, happy_counts from top_5_industry limit 5
        """)
        fourth_problem.show(100, truncate=False)
        fourth_problem.repartition(1).write.mode("overwrite").option("header", "true").csv(
            "/home/lumiq/Documents/test_data/fourth_problem/")
        '''
        FIFTH PROBLEM: Which 4 companies work a lot per week?
        '''
        fifth_problem = spark.sql("""SELECT Company_Name,MAX(Max_Actual_Hours_Per_Week) as actual_max_number_of_hours from for_all_problems where Company_Name !='' group by Company_Name 
        order by MAX(Max_Actual_Hours_Per_Week) desc limit 4
                                  """)
        fifth_problem.show(100, truncate=False)
        fifth_problem.repartition(1).write.mode("overwrite").option("header", "true").csv(
            "/home/lumiq/Documents/test_data/fifth_problem/")
        return ("Success")
