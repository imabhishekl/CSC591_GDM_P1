from graphframes import *
from pyspark.sql import SQLContext,Row
from pyspark import SparkContext, SparkConf
import sys

conf = SparkConf().setAppName("Articulation Points Finder").setMaster('local')
sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

def articulations(gf):
    articulation_points = list()
    gf_edgeDF = gf.edges
    gf_vertexDF = gf.vertices
    vertex_list = gf_vertexDF.rdd.map(lambda x: x[0]).collect()
    gf_edgeDF.cache()
    gf_vertexDF.cache()
    no_of_connected_components = gf.connectedComponents().select('component').distinct().count()
    for vertex in vertex_list:
        temp_edgeDF = gf_edgeDF.\
            where(gf_edgeDF.dst != vertex).\
            where(gf_edgeDF.src != vertex)
        temp_vertexDF = gf_vertexDF.\
            where(gf_vertexDF.id != vertex)
        temp_no_of_connected_components = GraphFrame(temp_vertexDF, temp_edgeDF).\
            connectedComponents().\
            select('component').distinct().count()
        if temp_no_of_connected_components > no_of_connected_components:
            articulation_points.append((vertex,1))
            print vertex,1
        else:
            articulation_points.append((vertex, 0))
    articulation_rdd= sc.parallelize(articulation_points).map(lambda x: Row(id=x[0],articulation=x[1]))
    articulation_df = sqlContext.createDataFrame(articulation_rdd)
    gf_edgeDF.unpersist()
    gf_vertexDF.unpersist()
    return articulation_df


edge_RDD = sc.textFile(str(sys.argv[1])).map(lambda x: x.split(',')).map(lambda x: Row(src=x[0], dst=x[1]))
edge_DF = sqlContext.createDataFrame(edge_RDD)
edge_DF.registerTempTable("EdgeDF")
vertexDF = sqlContext.sql("SELECT distinct src as id from EdgeDF UNION SELECT distinct dst as id from EdgeDF")
g= GraphFrame(vertexDF, edge_DF)
articulation_df = articulations(g)
articulation_df.show()

sc.stop()
exit()
