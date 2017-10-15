package pgrank

import org.apache.spark.{SparkConf, SparkContext}
import scala.xml.{NodeSeq, XML}

object SparkPageRank {
  def main(args: Array[String]): Unit= {
    //val sparkconf = new SparkConf().setAppName("pagerank").setMaster("local[4]")
    //val sc = new SparkContext(sparkconf)
    val sc = new SparkContext(new SparkConf())
    val hdfs = args(0)
    val input = args(1)
    val iter = args(2).toInt
    val testFile = sc.textFile(hdfs+input)
    val Pairs = testFile.map(line=>{
      val fields = line.split("\t")
      val (title, body) = (fields(1), fields(3))
      //println(body.size)
      val links =
        if (body == "\\N")
          NodeSeq.Empty
          //println(title)
        else
          try {
            XML.loadString(body) \\ "link" \ "target"
          } catch {
            case e: org.xml.sax.SAXParseException =>
              System.err.println("Article \""+title+"\" has malformed XML in body:\n"+body)
              NodeSeq.Empty
          }
      val linkSet = links.map(_.text).filter(tup=>tup!=title).toArray
      val id = title
      (id, linkSet)
    })
    var ranks = Pairs.mapValues(v => 1.0)
    println("finish loading data")
    for (i <- 1 to iter) {
      val contribs = Pairs.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    println("finish calculation")
    val Results = ranks.sortBy(_._2,false,1).take(100)
     /* .map(tup=>{
      val output = tup._2 + " has rank: "+ tup._1.toString
      (output)
    })*/
    sc.parallelize(Results).coalesce(1).saveAsTextFile(hdfs+"output/")
    //sc.parallelize(ranks)
    sc.stop()
  }
}
