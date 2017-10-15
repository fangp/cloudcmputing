package pgrank

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{GraphLoader, VertexId}

import scala.xml.{NodeSeq, XML}

object pre {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf())
    val hdfs = args(0)
    val input = args(1)
    val iter = args(2).toInt
    val mode = args(3).toInt
    val testFile = sc.textFile(hdfs+input)
    //val iters = args(3).toInt
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
      val linkSet = links.map(_.text).filter(tup=>tup!=title).toList
      val id = title
      (id, linkSet)
    })
    val Vertices = Pairs.keys.distinct()
    val Edges = Pairs.flatMap{case(id, linkSet)=>linkSet.map(link=>(id,link))}.map(tup=>{
      val output = Hash(tup._1)+"\t"+Hash(tup._2)
      (output)
    })
    Vertices.coalesce(1).saveAsTextFile(hdfs+"Vertices/")
    Edges.coalesce(1).saveAsTextFile(hdfs+"Edges/")
    sc.stop()
  }
  def Hash(title: String): VertexId ={
    title.toLowerCase.replace(" ", "").hashCode.toLong
  }
}
