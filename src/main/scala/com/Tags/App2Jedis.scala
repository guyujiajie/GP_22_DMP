package com.Tags

import org.apache.spark.{SparkConf, SparkContext}

object App2Jedis {
   def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      val sc = new SparkContext(conf)

      val dict = sc.textFile("dir/app_dict")
      dict.map(_.split("\t",-1))
         .filter(_.length>=5).foreachPartition(arr=>{

         val jedis = JedisConnectionPool.getConnection()
         arr.foreach(arr=>{
            jedis.set(arr(4),arr(1))
         })
         jedis.close()
      })
      sc.stop()
   }

}
