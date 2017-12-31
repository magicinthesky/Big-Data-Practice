from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("question4sql") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
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
    # apply schemas and create data frames for business and review
    df_bus = spark.createDataFrame(business, schema_business)
    df_rev = spark.createDataFrame(review, schema_review)

    # get columns containing "Palo Alto" only
    df_palo = df_bus.distinct().filter(df_bus.address.like("%Palo Alto%"))
    # select businessid,userid, stars from df_rev
    df_rev_select = df_rev.select(df_rev["businessid"], df_rev["userid"], df_rev["stars"])

    df_result = df_palo.join(df_rev_select, df_rev_select["businessid"]==df_palo["businessid"])
    df_result_select = df_result.select(df_rev["userid"], df_result["stars"].cast("float")
                                        .alias("ratings")).groupBy(df_rev["userid"]).avg("ratings")
    df_result_select.repartition(1).write.save("q4SQLoutput", "csv", "overwrite")
