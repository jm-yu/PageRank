package parse

import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup

import scala.collection.mutable.ListBuffer



object FileParse {

  def main(args: Array[String]): Unit = {
    /*
    input file format:
    <id> <title> <date> <XML> <plain text>
    ...
    ...
    <id> <title> <date> <XML> <plain text>

    output file format:
    <page_vertexID> <page_vertexID>
    ...
    ...
    <page_vertexID> <page_vertexID>
    */

    if (args.length < 3) {
      System.err.println(
        "Usage: PageRank <inputFile> <outputFile>")
      System.exit(-1)
    }

    val inputFile = args(0)
    val conf = new SparkConf().setAppName("Parse File")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    // parse input file
    val links = input.flatMap(file => file.split("\n")).map { line =>
      val contents = line.split("\t")
      val urls = Jsoup.parse(contents(3)).getElementsByTag("target")
      val result = new ListBuffer[String]()
      for (idx <- 0 to urls.size() - 1) {
        result += urls.get(idx).text()
      }
      (contents(1), result.filter(p => p != contents(1)).toArray)
    }
    // build all edges
    val edges = links.flatMap{ case (src_url, urls) =>
      val src = src_url.toLowerCase().replace(" ", "").hashCode.toLong
      urls.map(url => (src, url.toLowerCase().replace(" ", "").hashCode.toLong))
    }
    // save edges in file
    edges.coalesce(1).map(item => item.productIterator.mkString("\t")).saveAsTextFile(args(1))
  }
}
