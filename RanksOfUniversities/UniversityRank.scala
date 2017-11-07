package university

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}



object UniversityRank {

  def main(args: Array[String]): Unit = {
    /*
    inputFile format:
    <page_vertexID> <page_vertexID>

    university_name_file format:
    <loacation> <university name> <website>

    outpuFile format:
    <title> <rank>
    
    */


    if (args.length < 4) {
      System.err.println(
        "Usage: UniversityRank <inputFile> <outputFile> <university_name_file> <No of iterations>")
      System.exit(-1)
    }
    val iters = args(3).toInt
    val conf = new SparkConf().setAppName("Unviersity Rank")
    val sc = new SparkContext(conf)
    // load file and compute pagerank
    val graph = GraphLoader.edgeListFile(sc, args(1))
    val ranks = graph.staticPageRank(iters).vertices



    val univ = sc.textFile(args(0)).map{line =>
      val contents = line.split(",")
      // in case that some university name contains ","
      if (contents.size > 3) {
        contents(1) = contents(1) +", "+ contents(2)
        contents(2) = contents(3)
      }
      (contents(1).toLowerCase().replace(" ", "").hashCode.toLong, (contents(1) +"\t"+ contents(0) +"\t" + contents(2)))
    }

    // filter pagerank result with university names
    val rankByUni = univ.join(ranks).map{
      case (id, (uni, rank)) => (uni, rank)
    }
    
    //write result to output file
    val output = rankByUni.map(item =>item.swap).sortByKey(false,1).take(100)
    sc.parallelize(output).coalesce(1)saveAsTextFile(args(2))

  }
}

