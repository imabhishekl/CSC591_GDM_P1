from graphframes import *
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf


class Degree:
	sc = None
	conf = None
	degree_frequency = None
	sqlContext = None

	def setSC(self,appName,master):
		self.conf = SparkConf().setAppName(appName).setMaster(master)
		self.sc = SparkContext(conf=self.conf)

	def setSQLConextext(self):
		self.sqlContext=SQLContext(self.sc)

	def execute(self):
		in_file = self.sc.textFile("/home/abhishek/Downloads/network/9_11_edgelist.txt")
		op_rdd = in_file.map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[0],line[1])).collect()
		edge_df = self.sqlContext.createDataFrame(op_rdd,['src','dst'])
		edge_df.registerTempTable("edge_list")
		vertice_df = self.sqlContext.sql("SELECT distinct src as id from edge_list UNION SELECT distinct dst as id from edge_list")
		res_graph = GraphFrame(vertice_df,edge_df)
		res_graph.inDegrees.show()
		degree_list = res_graph.outDegrees
		degree_list.registerTempTable("degree")
		degree_count = self.sqlContext.sql("SELECT outDegree as degree,count(*) as count from degree group by outDegree order by outDegree")

		for i in degree_count.collect():
			print i
		self.degree_frequency = degree_count

def main():
	d = Degree()
	d.setSC('degree','local')
	d.setSQLConextext()
	d.execute()

if __name__ == '__main__':
	main()