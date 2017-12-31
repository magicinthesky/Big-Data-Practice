from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.sql.types import *
from pprint import pprint
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *


def getpairs(x):
    output = []
    for each in x[1].split(","):
        if len(each)>0:
            temp_pair = ",".join(sorted([x[0],each]))
            output.append((temp_pair, set(x[1].split(","))))
    return output

#0:userid, 1:firstname, 2:lastname, 3:address
def user_info(x):
    user = x.split(",")
    return user[0],user[1],user[2],user[3]

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("question1sql") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sc = spark.sparkContext
    rdd_input = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda x: x.split("\t"))
    rdd_new = rdd_input.flatMap(getpairs).reduceByKey(lambda v1, v2: v1.intersection(v2))
    rdd_result1 = rdd_new.filter(lambda x: len(x[1]) > 0)\
        .map(lambda x: (x[0].split(",")[0], x[0].split(",")[1],list(x[1])))
    # user1, user2, list of friends
    df_rdd1 = spark.createDataFrame(rdd_result1)
    count_friends = udf(lambda x: len(x), IntegerType())
    df_topten = df_rdd1.select("_1", "_2", count_friends("_3").alias("_3")).sort("_3", ascending=False).head(10)
    #pprint(df_topten)
    topten_string = "uid1 uid2 count"
    schema_topten = StructType([StructField(each, StringType(), True) for each in topten_string.split()])
    # apply schemas and create data frames for top10
    df_top10 = spark.createDataFrame(sc.parallelize(df_topten), schema_topten)
    # apply schemas and create data frames for userdata
    rdd_user = sc.textFile("userdata.txt").map(user_info)
    user_string = "uid fname lname address"
    schema_user = StructType([StructField(each, StringType(), True) for each in user_string.split()])
    df_user = spark.createDataFrame(rdd_user, schema_user)

    result_first = df_top10.join(df_user, df_user["uid"]==df_top10["uid1"], "cross")
    first_half = result_first.selectExpr("count as count", "uid as uid", "fname as fname1",
                                  "lname as lname1", "address as address1","uid2 as uid2")

    result_second = first_half.join(df_user, df_user["uid"]==first_half["uid2"],"cross")
    new_result = result_second.select(first_half["count"], first_half["fname1"],first_half["lname1"],
                                      first_half["address1"],df_user["fname"], df_user["lname"], df_user["address"])
    new_result.repartition(1).write.save('q2SQLoutput', 'csv', 'overwrite')


