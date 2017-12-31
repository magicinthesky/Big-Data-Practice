from pyspark import SparkContext, SparkConf
from pprint import pprint


def business_info(x):
    bus = x.split("::")
    return bus[0], (bus[1], bus[2])

def review_info(x):
    rev = x.split("::")
    return rev[2], rev[1]


if "__main__" == __name__:
    conf = SparkConf().setAppName("question3").setMaster('local')
    sc = SparkContext(conf=conf)
    # "business_id"::"full_address"::"categories"
    business = sc.textFile("business.csv")
    # "review_id"::"user_id"::"business_id"::"stars"
    review = sc.textFile("review.csv")
    rdd_bus = business.map(business_info)
    rdd_rev = review.map(review_info)
    rdd_count = rdd_rev.map(lambda x: (x[0], 1)).reduceByKey(lambda v1, v2: v1 + v2)\
        .sortBy(lambda x: x[1], ascending=False, numPartitions=1)

    #(bus_id, ((addr,cat), num))
    rdd_topten = rdd_bus.join(rdd_count).distinct().top(10, key=lambda x: x[1][1])
    rdd_format = sc.parallelize(rdd_topten)
    rdd_result = rdd_format.map(lambda x: str(x[0])+"\t"+str(x[1][0][0])+"\t"+str(x[1][0][1])+"\t"+str(x[1][1]))
    rdd_result.saveAsTextFile("q3output")
