from graphframes import *
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf


class Degree:
	sc = None
	conf = None
	degree_frequency = None
	sqlContext = None
	res_graph = None


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
		self.res_graph = GraphFrame(vertice_df,edge_df)
		
	def removeVertex(self):
		

def main():
	d = Degree()
	d.setSC('degree','local')
	d.setSQLConextext()
	d.execute()
	d.shortestPath('Hani Hanjour','Zakariya Essabar')

if __name__ == '__main__':
	main()