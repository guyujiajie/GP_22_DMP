package com.Tags

import com.utils.{AmapUtil, Tag}
import org.apache.spark.sql.Row
import ch.hsr.geohash.GeoHash
import org.apache.commons.lang.StringUtils
import redis.clients.jedis.Jedis

/**
  * 商圈标签
  * 获取key用geohash算法,见项目文档
  * 缓存数据库用redis
  */
object BusinessTag extends Tag{

      /**
        * override def makeTags(args: Any*): List[(String, Int)] = {
        * var list = List[(String,Int)]()
        * // 处理参数类型
        * val row = args(0).asInstanceOf[Row]
        * val appmap = args(1).asInstanceOf[Broadcast[Map[String, String]]]
        * // 获取APPid和APPname
        * val appname = row.getAs[String]("appname")
        * val appid = row.getAs[String]("appid")
        * // 空值判断
        * if(StringUtils.isNotBlank(appname)){
        * list:+=("APP"+appname,1)
        * }else if(StringUtils.isNotBlank(appid)){
        * list:+=("APP"+appmap.value.getOrElse(appid,appid),1)
        * }
        * list
        * }
        */
   /**
     * 打标签的同一接口
     */
   override def makeTags(args: Any*): List[(String, Int)] = {

      var list: List[(String, Int)] = List[(String,Int)]()
      //解析参数
      val row: Row = args(0).asInstanceOf[Row]

      //获取经纬度
      val long: String = row.getAs[String]("long")
      val lat: String = row.getAs[String]("lat")
      //过滤符合条件的
      if(long.toDouble >= 73 && long.toDouble <=135
         && lat.toDouble >=3 && lat.toDouble <= 54) {
         //获取商圈信息
         val business: String = getBusiness(long.toDouble,lat.toDouble)
         //判断缓存中是否有商圈信息
         if(StringUtils.isNotBlank(business)) {
            val lines: Array[String] = business.split(",")
            lines.foreach(x=>list:+=(x,1))
         }

      }
      list
   }

   /**
     * 获取商圈信息
     */

   def getBusiness(long:Double,lat:Double):String={

      //转换geohash字符串，导入依赖
      val geohash: String = GeoHash.geoHashStringWithCharacterPrecision(lat,long,8 )

      val business: String = redis_queryBusiness(geohash)
      if(business ==null || business ==0) {
         //创建商圈
         val business: String = AmapUtil.getBusinessFromAmap(long.toDouble,lat.toDouble)
         //存入数据库
         val str: String = redis_insertBusiness(geohash,business)

      }
      business
   }

   /**
     * 查询数据库信息
     */
   def redis_queryBusiness(geohash:String):String = {
      val jedis: Jedis = JedisConnectionPool.getConnection()
      val business: String = jedis.get(geohash)
      jedis.close()
      business
   }

   /**
     * 将商圈存储到redis
     */
   def redis_insertBusiness(geohash:String,business:String):String={
      val jedis: Jedis = JedisConnectionPool.getConnection()
      val business: String = jedis.set(geohash,business)
      jedis.close()
      business
   }
}
