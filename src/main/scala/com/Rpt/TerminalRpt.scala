package com.Rpt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TerminalRpt {
   def main(args: Array[String]): Unit = {

      val Array(inputPath,outputPath)=args
      //判断路径
      if(args != 2){
         println("目录参数不正确，退出请即可修改")
         sys.exit()
      }

//      val spark = SparkSession.builder()
//         .appName(this.getClass.getName)
//         .master("local[*]")
//         .getOrCreate()
//         .conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//

      val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
         .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

      val sc = new SparkContext(conf)

      val spark = SparkSession.builder().config(conf).getOrCreate()

      //读取文件为DataFrame
      import spark.implicits._

      val df: DataFrame = spark.read.parquet("/Users/guyujiajie/Documents/IDEAOutput/txt2ParquetOut/part-00000-0ca0441b-e8e7-4682-b924-f5d4664bf19c-c000.snappy.parquet")
      //创建临时表
      df.createOrReplaceTempView("terminal")

      spark.sql("select networkmannername," +
         "sum(case when REQUESTMODE >= 1 and REQUESTMODE <= 3 then 1 else 0 end) `原始请求数`," +
         "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 2 then 1 else 0 end) `有效请求数`," +
         "sum(case when REQUESTMODE = 1 and PROCESSNODE = 3 then 1 else 0 end) `广告请求数`," +
         "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISBID = 1 then 1 else 0 end) `参与竞价数`," +
         "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 and ADORDERID != 0 then 1 else 0 end) `竞价成功数`," +
         "sum(case when REQUESTMODE = 2 and ISEFFECTIVE = 1 then 1 else 0 end) `展示数`," +
         "sum(case when REQUESTMODE = 3 and ISEFFECTIVE = 1 then 1 else 0 end) `点击数`," +
         "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告消费`," +
         "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告成本`" +
         " from " +
         "log group by networkmannername").show()

      spark.stop()
   }

}
