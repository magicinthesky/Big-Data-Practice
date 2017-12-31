from pyspark import SparkContext,SparkConf
from pprint import pprint

def getpairs(x):
    output = []
    for each in x[1].split(","):
        if len(each)>0:
            temp_pair = tuple(sorted([x[0],each]))
            output.append((temp_pair, set(x[1].split(","))))
    return output

#0:userid, 1:firstname, 2:lastname, 3:address
def user_info(x):
    user = x.split(",")
    return user[0], (user[1],user[2],user[3])

def first_format(x):
    pair = x[0]
    friend_a = x[0][0]
    return friend_a, (x[1], pair)

def second_format(x):
    pair = x[0]
    friend_b = x[0][1]
    return friend_b, (x[1], pair)

if "__main__" == __name__:
    conf = SparkConf().setAppName("question2").setMaster('local')
    sc = SparkContext(conf=conf)
    rdd_input = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda x: x.split("\t"))

    rdd_new = rdd_input.flatMap(getpairs).reduceByKey(lambda v1, v2: v1.intersection(v2))
    rdd_topten = rdd_new.map(lambda x: [x[0], len(x[1])]).top(10, key=lambda x: x[1])
    rdd_topten1 = sc.parallelize(rdd_topten)
    # pprint(rdd_topten) ok
    rdd_user = sc.textFile("userdata.txt").map(user_info)
    #npprint(rdd_user.take(5)) ok
    # (u'0', (u'Evangeline', u'Taylor', u'3396 Rogers Street'))
    first_half = rdd_topten1.map(first_format).join(rdd_user).map(lambda x: x[1])
    second_half = rdd_topten1.map(second_format).join(rdd_user).map(lambda x: x[1])
    rdd_result = first_half.join(second_half)
    rdd_result1 = rdd_result.map(
        lambda x: "\t".join([str(x[0][0]),str(x[1][0][0]), str(x[1][0][1]), str(x[1][0][2]),str(x[1][1][0]),
                             str(x[1][1][1]), str(x[1][1][2])]))
    rdd_result1.repartition(1).saveAsTextFile("q2output")




