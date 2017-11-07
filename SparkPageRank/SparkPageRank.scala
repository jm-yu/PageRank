package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup

import scala.collection.mutable.ListBuffer

object SparkPageRank {

  def main(args: Array[String]): Unit = {
    /*
    input file format:
    <id> <title> <date> <XML> <plain text>
    ...
    ...
    <id> <title> <date> <XML> <plain text>

    output file format:
    <title> <rank>
    ...
    ...
    <title> <rank>
    */

    if (args.length < 3) {
      System.err.println(
        "Usage: SparkPageRank <inputFile> <outputFile> <No of iterations>")
      System.exit(-1)
    }

    val inputFile = args(0)
    val iters = args(2).toInt
    val conf = new SparkConf().setAppName("Spark PageRank")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)

    // parse a file:
    // each line of input file => page_i + list of pages that page_i jumps to.
    val links = input.flatMap(file => file.split("\n")).map { line =>
      val contents = line.split("\t")
      val urls = Jsoup.parse(contents(3)).getElementsByTag("target")
      val result = new ListBuffer[String]()
      for (idx <- 0 to urls.size() - 1) {
        result += urls.get(idx).text()
      }
      (contents(1), result.filter(p => p != contents(1)).toArray)
    }
    // init ranks
    var ranks = links.mapValues(v => 1.0)
    // compute ranks
    for (i <- 1 to iters) {
      val url_ranks = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.length
        urls.map(url => (url, rank / size))
      }
      ranks = url_ranks.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    // write to output file
    val output = ranks.map(item =>item.swap).sortByKey(false).take(100)
    sc.parallelize(output).coalesce(1)saveAsTextFile(args(1))
  }
}
