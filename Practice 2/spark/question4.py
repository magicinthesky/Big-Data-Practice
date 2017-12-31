from pyspark import SparkContext,SparkConf


def business_info(x):
    bus = x.split("::")
    return bus[0], (bus[1], bus[2])

def review_info(x):
    rev = x.split("::")
    return rev[2], (rev[1], rev[3])

if "__main__" == __name__:
    conf = SparkConf().setAppName("question4").setMaster("local")
    sc = SparkContext(conf=conf)
    # "business_id"::"full_address"::"categories"
    business = sc.textFile("business.csv")
    # "review_id"::"user_id"::"business_id"::"stars"
    review = sc.textFile("review.csv")

    # get columns containing "Palo Alto" only
    rdd_bus = business.map(business_info).filter(lambda x: "Palo Alto" in x[1][0])
    rdd_rev = review.map(review_info)
    # join business and review
    # uid=x[1][0][0], stars=x[1][0][1]
    # reduceByKey: (total stars, num of reviews)
    rdd_new = rdd_rev.join(rdd_bus).map(lambda x: (x[1][0][0], x[1][0][1])).\
        mapValues(lambda x: (x, 1)).reduceByKey(lambda v1, v2: (str(float(v1[0]) + float(v2[0])), v1[1] + v2[1]))
    # get average ratings
    rdd_result = rdd_new.map(lambda x: (str(x[0])+"\t"+str(float(x[1][0]) / x[1][1])))
    rdd_result.repartition(1).saveAsTextFile("q4output")



