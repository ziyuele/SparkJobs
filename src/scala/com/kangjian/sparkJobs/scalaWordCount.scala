package com.kangjian.sparkJobs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object scalaWordCount {

  private def usage(): Unit = {
    println("spark-submit --class com.kangjian.sparkJobs XXX.jar <inputfilePath>")
  }

  private def calcu(str: String):Unit={
    val conf = new SparkConf().setAppName("scala work count")
    val sc = new SparkContext(conf)
    val ans = sc.textFile(str).flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((x,y) => (x + y))
    ans.foreach(x => println(x))

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    var path = ""
    if (args.length > 0) path = args(0) else usage()
    calcu(path)

    println("all work has finished")
  }
}
