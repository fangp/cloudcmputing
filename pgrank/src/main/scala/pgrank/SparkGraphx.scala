package pgrank

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.xml.{NodeSeq, XML}

object SparkGraphx {
  def main(args: Array[String]): Unit = {
    //val sparkconf = new SparkConf().setAppName("pagerank").setMaster("local[4]")
    //val sc = new SparkContext(sparkconf)
    val sc = new SparkContext(new SparkConf())
    val testFile = sc.textFile(args(1))
    val iters = args(3).toInt
    val Pairs = testFile.map(line => {
      val fields = line.split("\t")
      val (title, body) = (fields(1), fields(3))
      val links =
        if (body == "\\N")
          NodeSeq.Empty
        //println(title)
        else
          try {
            XML.loadString(body) \\ "link" \ "target"
          } catch {
            case e: org.xml.sax.SAXParseException =>
              System.err.println("Article \"" + title + "\" has malformed XML in body:\n" + body)
              NodeSeq.Empty
          }
      val linkSet = links.map(_.text).toList
      val id = title
      (id, linkSet)
    })
    val vertices = Pairs.map(a => (Hash(a._1), a._1))
    val edgeList: RDD[Edge[Double]] = Pairs.flatMap { a =>
      val src = Hash(a._1)
      a._2.map { t => Edge(src, Hash(t), 1.0 / a._2.size)
      }
    }
    val graph = Graph(vertices, edgeList)
    val pgRank = graph.staticPageRank(iters)
    val tG = graph.outerJoinVertices(pgRank.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }
    val result = tG.vertices.map(tup=>(tup._2._1, tup._2._2)).sortByKey(false, 1).take(100)
    sc.parallelize(result).coalesce(1).saveAsTextFile(args(2))
  }
  def Hash(title: String): VertexId = {
    title.toLowerCase.replace(" ", "").hashCode.toLong
  }
}
