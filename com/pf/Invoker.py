from pyspark.sql import SparkSession
from com.pf.Data_1 import Data_1
from com.pf.Data_2 import Data_2
from com.pf.Data_3 import Data_3
from com.pf.Aggregations import Aggregations


def main():
    spark = SparkSession.builder \
        .master('local[*]') \
        .appName('my-cool-app') \
        .getOrCreate()

    # Input paths...
    path_data_1 = "/home/lumiq/Documents/salary_data/data_1.csv"
    path_data_2 = "/home/lumiq/Documents/salary_data/data_2.csv"
    path_data_3 = "/home/lumiq/Documents/salary_data/data_3.csv"

    obj = Data_1()
    (result, bad_records_data_1, transformed_data_1) = obj.Start(spark, path_data_1)

    obj = Data_2()
    (result, transformed_data_2, bad_records_data_2) = obj.Start(spark, path_data_2)

    obj = Data_3()
    (result, transformed_data_3, bad_records_data_3) = obj.Start(spark, path_data_3)

    obj = Aggregations()
    (result) = obj.start_aggregations(spark, transformed_data_1, transformed_data_2, transformed_data_3)


if __name__ == '__main__':
    main()
