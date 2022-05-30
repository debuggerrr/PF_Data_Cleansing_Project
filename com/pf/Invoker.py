from pyspark.sql import SparkSession
from com.pf.Data_1 import Data_1
from com.pf.Data_2 import Data_2
from com.pf.Data_3 import Data_3


def main():
    spark = SparkSession.builder \
        .master('local[*]') \
        .config("spark.driver.memory", "8g") \
        .appName('my-cool-app') \
        .getOrCreate()

    # Input paths...
    path_data_1 = "/home/lumiq/Documents/salary_data/data_1.csv"
    path_data_2 = "/home/lumiq/Documents/salary_data/data_2.csv"
    path_data_3 = "/home/lumiq/Documents/salary_data/data_3.csv"

    obj = Data_1(spark)
    (result, bad_records_data_1, transformed_data_1) = obj.Start(spark, path_data_1)
    transformed_data_1.show(100, truncate=False)

    obj = Data_2(spark)
    (result, transformed_data_2, bad_records_data_2) = obj.Start(spark, path_data_2)
    transformed_data_2.show(400, truncate=False)

    obj = Data_3(spark)
    (result, transformed_data_3, bad_records_data_3) = obj.Start(spark, path_data_3)
    transformed_data_3.show(400, truncate=False)



if __name__ == '__main__':
    main()
