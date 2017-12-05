package com.kangjian.sparkJobs

import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.SparkSession

class ScalaAccumulator {

  def usage(): Unit ={
    println("spark-submit --class com.kangjian.sparkJobs XXX.jar <inputfilePath>")
    sys.exit(1)
  }
  def calcu(sc: SparkContext, path: String): Unit ={

    val acc = sc.accumulator(0)
    val file = sc.textFile(path)
    val step1 = file.flatMap(x => x.split(" ")).map(w => (w,1)).reduceByKey{ (a, b) =>
      acc += 1
      a+b
    }
    print("work without shuffle acc is :" + acc)
    step1.collect()
    print("work whth shuffle acc is :" + acc )
    sc.stop()
  }
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("dfad").setMaster("local")
    val sc = new SparkContext(conf)
    if (args.length > 0) calcu(sc, args(0)) else usage()
  }
}
