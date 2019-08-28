package com.ZhouKao

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object BusinessWeek {
   def main(args: Array[String]): Unit = {

//      if(args.length != 2) {
//         println("目录数量不正确")
//         sys.exit()
//      }
//      val Array(inputPath,outputPath)=args
      val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      val sc = new SparkContext(conf)

      //读取json数据文件
      val data: RDD[String] = sc.textFile("dir/json.txt")

      data.map(line =>{
         BusinessArea.getData(line)
      })
         .filter(!_.contains("[]"))
         .map(x => {
            val arr = x.split(",")

            (arr(2),arr.length)
         }).foreach(println)


   }
}
