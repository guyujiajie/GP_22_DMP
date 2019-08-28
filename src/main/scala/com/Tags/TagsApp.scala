package com.Tags

import com.utils.Tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis


object TagsApp extends Tag {

//   override def makeTags(args: Any*): List[(String, Int)] = {
//      var list = List[(String, Int)]()
//      val row = args(0).asInstanceOf[Row]
//      val AppName: String = row.getAs[String]("appname")
//
//      val appid = row.getAs[String]("appid")

//      if (StringUtils.isNotBlank(AppName)) {
//         list :+= ("APP" + AppName, 1)
//      } else if (StringUtils.isBlank(AppName)) {
//         var jedis = new Jedis("NODE03")
//         val tmp = jedis.get(appid)
//         list :+= ("appid" + tmp, 1)
//      }
//      list
//   }

   override def makeTags(args: Any*): List[(String, Int)] = {
      var list = List[(String,Int)]()
      // 处理参数类型
      val row = args(0).asInstanceOf[Row]
      val appmap = args(1).asInstanceOf[Broadcast[Map[String, String]]]
      // 获取APPid和APPname
      val appname = row.getAs[String]("appname")
      val appid = row.getAs[String]("appid")
      // 空值判断
      if(StringUtils.isNotBlank(appname)){
         list:+=("APP"+appname,1)
      }else if(StringUtils.isNotBlank(appid)){
         list:+=("APP"+appmap.value.getOrElse(appid,appid),1)
      }
      list
   }
}
