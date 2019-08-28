package com.ZhouKao


import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
object Type {

   var list:List[String] = List()
   def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      val sc = new SparkContext(conf)

      //获取数据文件
      val data: RDD[String] = sc.textFile("dir/json.txt")

      val logs: mutable.Buffer[String] = data.collect().toBuffer
      logs.map(line => {
         val jsonparse: JSONObject = JSON.parseObject(line)
         //寻找
         val status = jsonparse.getIntValue("status")
         if (status == 0) return ""
         //逐层寻找
         val regeocodeJson = jsonparse.getJSONObject("regeocode")
         if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

         val poisArray = regeocodeJson.getJSONArray("pois")
         if (poisArray == null || poisArray.isEmpty) return null

         // 创建集合 保存数据
         val buffer = collection.mutable.ListBuffer[String]()
         // 循环拼接取得的type
         for (item <- poisArray.toArray) {
            if (item.isInstanceOf[JSONObject]) {
               val json = item.asInstanceOf[JSONObject]
               buffer.append(json.getString("type"))
            }
         }
         //拼接为xx；xx；xx
         list:+=buffer.mkString(",")
      })
      val res: List[(String, Int)] = list.flatMap(x => x.split(","))
         .map(x => {
            val arr = x.split(";")
            ("type:"+arr(1),1)
         })
         .groupBy(x => x._1)
         .mapValues(x => x.size).toList.sortWith(_._2 > _._2)

      res.foreach(x => println(x))
   }
}