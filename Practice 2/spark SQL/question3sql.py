from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("question3sql"). \
        config("spark.some.config.option", "some-value").\
        getOrCreate()

    sc = spark.sparkContext
    # "business_id"::"full_address"::"categories"
    business = sc.textFile("business.csv").map(lambda x: x.split("::"))
    # "review_id"::"user_id"::"business_id"::"stars"
    review = sc.textFile("review.csv").map(lambda x: x.split("::"))

    # create schemas for business and review
    bus = "businessid address category"
    rev = "reviewid userid businessid stars"
    schema_business = StructType([StructField(each, StringType(), True) for each in bus.split()])
    schema_review = StructType([StructField(each, StringType(), True) for each in rev.split()])
    df_rev = spark.createDataFrame(review, schema_review)
    df_bus = spark.createDataFrame(business, schema_business)

    df_rev_count = df_rev.distinct().groupBy("businessid").count()

    rdd_topten = df_rev_count.sort("count", ascending=False).head(10)
    rdd_topten = spark.createDataFrame(sc.parallelize(rdd_topten))

    # join
    df_result = rdd_topten.join(df_bus, rdd_topten["businessid"] == df_bus["businessid"])
    df_result_select = df_result.select(df_bus["businessid"], df_bus["address"], df_bus["category"], "count")\
        .distinct().orderBy("count", ascending=False)
    df_result_select.repartition(1).write.save("q3SQLoutput", "csv", "overwrite")
