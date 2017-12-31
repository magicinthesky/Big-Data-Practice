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

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("question1sql") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sc = spark.sparkContext
    rdd_input = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda x: x.split("\t"))
    rdd_new = rdd_input.flatMap(getpairs).reduceByKey(lambda v1, v2: v1.intersection(v2))
    rdd_result = rdd_new.filter(lambda x: len(x[1]) > 0)\
        .map(lambda x: (x[0].split(",")[0], x[0].split(",")[1],list(x[1])))
    # pprint(rdd_result.take(2))
    # user1, user2, list of friends
    df_rdd = spark.createDataFrame(rdd_result)
    #df_rdd.printSchema()
    count_friends = udf(lambda x: len(x), IntegerType())
    df_result = df_rdd.select("_1", "_2", count_friends("_3")).\
        repartition(1).write.save('q1SQLoutput', 'csv', 'overwrite')

