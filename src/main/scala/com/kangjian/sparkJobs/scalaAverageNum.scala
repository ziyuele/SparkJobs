package com.kangjian.sparkJobs

import org.apache.spark.{SparkConf, SparkContext}

object scalaAverageNum {
    private def usage(): Unit = {
      println("spark-submit --class com.kangjian.sparkJobs XXX.jar <inputfilePath>")
    }

    def calcu(str: Array[(String,Double)]): Unit ={
      val conf = new SparkConf().setAppName("scala average")
      val ctx = new SparkContext(conf)
      ctx.parallelize(str).combineByKey(
        //操作的是value
        //Func => createCombiner
        v => (v, 1),
        //操作的是本地块的数据
        //Func => mergeValue
        (arr:(Double,Int), v) => (arr._1 + v , arr._2 + 1),
        //整个块的数据聚合
        //Func => createCombiner
        (arr1:(Double,Int), arr2:(Double,Int)) => (arr1._1 + arr2._1, arr1._2 + arr2._2)
      )

    }
    def main(args:Array[String]): Unit ={
      val array  = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
      if (args.length == 0) usage() else calcu(array)
     }
}
