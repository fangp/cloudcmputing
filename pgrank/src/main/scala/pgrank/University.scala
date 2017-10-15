package pgrank

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{GraphLoader, VertexId}

object University {
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
    val univ = sc.textFile(hdfs+"input/world-universities.csv").map{line =>{
      val field = line.split(",")
      var name1 = field(1)
      if(field.size>3)
        name1= name1+", "+field(2)
      val name = Hash(name1)
      val country = " Country: "+field(0)

      val website = " Website: "+field(field.size-1)
      (name, name1+"\t"+country+"\t"+website)
    }
    }
    val ranksByUsername = univ.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }.map(item=>item.swap).sortByKey(false,1).take(100)
    val result = ranksByUsername.map(tup=>(tup._2+"\t Weights:"+tup._1))
    sc.parallelize(result).coalesce(1).saveAsTextFile(hdfs+"output2/")
    sc.stop()
  }
  def Hash(title: String): VertexId ={
    title.toLowerCase.replace(" ", "").hashCode.toLong
  }
}
