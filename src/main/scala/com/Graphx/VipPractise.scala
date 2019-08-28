package com.Graphx

import java.util.Properties

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object VipPractise {
   def main(args: Array[String]): Unit = {

      val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      val sc: SparkContext = new SparkContext(conf)
      val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

//      val list: List[String] = List("1	小红 20","3 小明 33","5	小七	20","7	小王 60","9 小李	20","11	小美	30","13 小花")
//
//      val vertexRDD: RDD[(Long, (String, String))] = sc.makeRDD(list).map(x => {
//         val arr: Array[String] = x.split("\\s")
//         if (arr.length == 3) {
//            (arr(0).toLong, (arr(1), arr(2)))
//         } else null
//      }).filter(_ != null)

      //米哥的答案
      val rdd: RDD[String] = sc.makeRDD(List("1	小红 20","3 小明 33","5	小七	20","7	小王 60","9 小李	20","11	小美	30","13 小花"))
      val vertexRDD: RDD[(VertexId, (String, String))] = rdd.filter(_.split("\\s").length==3).map(t=>(t.split("\\s")(0).toLong,(t.split("\\s")(1),t.split("\\s")(2))))


      val edge: RDD[Edge[Int]] = sc.makeRDD(Seq(
         Edge(1L, 3L, 0),
//         Edge(5L, 3L, 0),
//         Edge(7L, 3L, 0),
         Edge(9L, 3L, 0),
         Edge(13L, 3L, 0),
//         Edge(5L, 13L, 0),
//         Edge(7L, 13L, 0),
         Edge(5L, 7L, 0),
         Edge(9L, 11L, 0)
      ))

      //构建图
      val graph: Graph[(String, String), Int] = Graph(vertexRDD,edge)
      //取出每个边上的最大顶点,实际是1号创始人
      val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
      vertices.join(vertexRDD).map {
         case (userId, (conId, (name, age))) => {
            (conId, List(name, age))
         }
      }.reduceByKey(_ ++ _).foreach(println)

//      val resDF: DataFrame = spark.createDataFrame(res)
//      /**
//        * 存入数据库
//        */
//
//      val pro: Properties = new Properties()
//      pro.put("user","root")
//      pro.put("password","123456")
//      val url = "jdbc:mysql://localhost:3306/sparkout?useUnicode=true&characterEncoding=UTF-8"
//      resDF.write.jdbc(url,"Vip1",pro)

   }
}