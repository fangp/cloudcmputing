package pgrank

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{GraphLoader, VertexId}

object SparkOnlyGraphx {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf())
    val hdfs = args(0)
    val input = args(1)
    val iter = args(2).toInt
    val mode = args(3).toInt
    val graph = GraphLoader.edgeListFile(sc, hdfs+"Edges/part-00000")
    println("finish loading edgelist")
    val ranks = graph.staticPageRank(iter).vertices
    println("finish calculation")
    val users = sc.textFile(hdfs+"Vertices/part-00000").map { line => {
      //val field = line.split(",")
      (Hash(line), line)
    }
    }.reduceByKey((a, b)=>a)
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }.sortBy(_._2,false,1).take(100)
    sc.parallelize(ranksByUsername).coalesce(1).saveAsTextFile(hdfs+"output2/")
    sc.stop()
  }
  def Hash(title: String): VertexId ={
    title.toLowerCase.replace(" ", "").hashCode.toLong
  }
}
