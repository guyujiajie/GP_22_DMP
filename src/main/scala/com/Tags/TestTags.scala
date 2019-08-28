package com.Tags

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object TestTags {
   def main(args: Array[String]): Unit = {
      def main(args: Array[String]): Unit = {
         val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
         val sc = new SparkContext(conf)
         val spark = SparkSession.builder().config(conf).getOrCreate()
         import spark.implicits._
         val df = spark.read.parquet("/Users/guyujiajie/Downloads/txt2ParquetOut/part-00000-0ca0441b-e8e7-4682-b924-f5d4664bf19c-c000.snappy.parquet")
         df.map(row=>{
            val business = BusinessTag.makeTags(row)
            business
            //      (v1,v2)
         }).rdd.foreach(println)
      }
   }

}
