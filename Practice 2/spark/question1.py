from pyspark import SparkContext,SparkConf

def getpairs(x):
    output = []
    for each in x[1].split(","):
        if len(each)>0:
            temp_pair = ",".join(sorted([x[0],each]))
            output.append((temp_pair, set(x[1].split(","))))
    return output


if "__main__" == __name__:
    conf = SparkConf().setAppName("question1").setMaster('local')
    sc = SparkContext(conf=conf)
    rdd_input = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda x: x.split("\t"))

    rdd_new = rdd_input.flatMap(getpairs).reduceByKey(lambda v1, v2: v1.intersection(v2))
    rdd_result = rdd_new.filter(lambda x: len(x[1]) > 0).map(lambda x: x[0]+"\t"+str(len(x[1])))
    rdd_result.saveAsTextFile("q1output")