from graphframes import *
from pyspark.sql import SQLContext,Row
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Centrality Closeness Finder").setMaster('local')
sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

#In general filter for graphs with disconnected graphs
def filterShortestPathRDD(rddelem,v1):
    if rddelem[1].__contains__(v1):
        if type(rddelem[1][v1]) is int:
            return True
    return False

def closeness(gf):
    closeness_values = list()
    #GraphFrame cached to speed up iterative computation
    vertex_list= gf.vertices.rdd.map(lambda x: x[0]).collect()
    gf.cache()
    for vertex in vertex_list:
        #Valid Path values summed up using reduce to get summation of Shortest Paths
        summation = gf.shortestPaths(landmarks=[vertex]).rdd.\
        filter(lambda x: filterShortestPathRDD(x,vertex)).\
        map(lambda x: x[1][vertex] ).\
        reduce(lambda x,y: x + y)
        print vertex,summation
        closeness_values.append((vertex,(1./summation)))
    closeness_rdd= sc.parallelize(closeness_values).map(lambda x: Row(id=x[0],closeness=x[1]))
    gf.unpersist()
    return sqlContext.createDataFrame(closeness_rdd)

#creating given Graph as GraphFrame
vertex_DF = sqlContext.createDataFrame([("A",), ("B",), ("C",), ("D",), ("E",), ("F",), ("G",),("H",),("I",),("J",)], ["id"])

edge_DF = sqlContext.createDataFrame([
    ("A","B"),
    ("A","C"),
    ("A","D"),
    ("B","A"),
    ("B","C"),
    ("B", "D"),
    ("B", "E"),
    ("C", "A"),
    ("C", "B"),
    ("C", "D"),
    ("C", "F"),
    ("C", "H"),
    ("D", "A"),
    ("D", "B"),
    ("D", "C"),
    ("D", "E"),
    ("D", "F"),
    ("D", "G"),
    ("E", "B"),
    ("E", "G"),
    ("E", "F"),
    ("E", "D"),
    ("F", "E"),
    ("F", "G"),
    ("F", "D"),
    ("F", "H"),
    ("F", "C"),
    ("G", "D"),
    ("G", "E"),
    ("G", "F"),
    ("H", "I"),
    ("H", "F"),
    ("H", "C"),
    ("I", "H"),
    ("I", "J"),
    ("J", "I")
], ["src","dst"])

g= GraphFrame(vertex_DF, edge_DF)

closeness_df = closeness(g)
closeness_df.orderBy("closeness",ascending=False).show()

sc.stop()
exit()
