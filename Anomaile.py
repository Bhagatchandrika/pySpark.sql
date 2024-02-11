from pyspark.sql import SparkSession
from pyspark.sql import *

def maximux_content_size(spark):
    df = spark.read.options(header='True', inferSchema='True') \
        .parquet("/user/test/data/apilogs.parquet")
    df.show()
    df_write = df.write.mode("overwrite").format("parquet").saveAsTable("default.apilogs_parquet")
    df1 = spark.sql("select * from apilogs_parquet")
    df1.show()
    df2 = spark.sql(
        "SELECT endpoint, SUM(content_size) AS total_content_size\
         FROM apilogs_parquet GROUP BY endpoint ORDER BY total_content_size DESC")
    df2.show()
    df3 = df2.limit(10)
    df3.show()

def maximum_visited_endpoint(spark):
    df = spark.read.options(header='True', inferSchema='True') \
        .parquet("/user/test/data/apilogs.parquet")
    df1 = spark.sql("select endpoint from apilogs_parquet GROUP BY endpoint ORDER BY endpoint DESC LIMIT 10")
    df1.show()

def daily_content_size(spark):
    df = spark.read.options(header='True', inferSchema='True') \
        .parquet("/user/test/data/apilogs.parquet")
    df1 = spark.sql("SELECT date AS daily, SUM(content_size) AS Total_count_size\
     FROM apilogs_parquet GROUP BY date ORDER BY date")
    df1.show()

def min_content_size(spark):

    df1 = spark.sql("select min(content_size) from apilogs_parquet where content_size != 0")
    df1.show()

def max_content_size(spark):
    df = spark.sql("select max(content_size) from apilogs_parquet")
    df.show()

def count_content_size(spark):
    df = spark.sql("select count(content_size) from apilogs_parquet")
    df.show()

#section 2:
def number_of_response_code(spark):
    df = spark.sql("select response_code, count(response_code) as numOfResponseCode\
     from apilogs_parquet group by response_code order by numOfResponseCode")
    df.show()

def ip_address_over_10_times(spark):
    df = spark.sql("select ip_address, count(ip_address) as visit_count from apilogs_parquet\
     group by ip_address\
     having visit_count >= 10\
     order by visit_count desc")
    df.show(truncate=False)
    df1 = spark.sql("select count(response_code) from apilogs_parquet where response_code = 404")
    df1.show()

# section 3
def bad_request(spark):
    df = spark.sql("select date, endpoint, response_code from apilogs_parquet\
     where response_code = 404 order by date desc, time desc limit 10")
    df.show(truncate=False)


if __name__ == '__main__':
        spark: SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").enableHiveSupport().getOrCreate()

        maximux_content_size(spark)
        maximum_visited_endpoint(spark)
        daily_content_size(spark)
        min_content_size(spark)
        max_content_size(spark)
        count_content_size(spark)
        number_of_response_code(spark)
        ip_address_over_10_times(spark)
        bad_request(spark)
