package com.Tags

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object TestContext {
   def main(args: Array[String]): Unit = {

      val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      val sc: SparkContext = new SparkContext(conf)
      val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

      val df: DataFrame = spark.read.parquet("/Users/guyujiajie/Documents/IDEAOutput/txt2ParquetOut/part-00000-0ca0441b-e8e7-4682-b924-f5d4664bf19c-c000.snappy.parquet")
      import spark.implicits._
      df.map(row =>{
         val business: List[(String, Int)] = BusinessTag.makeTags(row)
         business
      }).rdd.foreach(println)

   }

}
