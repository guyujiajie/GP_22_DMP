package com.utils

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 标签工具类
  * 为什么用object不用class
  * 所有的main方法必须在object中被调用
  * 使用object相当于java的单例对象
  * 方法在这里面自动初始化，可以直接调用,object中可以实现静态方法和静态对象的功能。
  * 常用工具类object创建，下次直接使用，不会像类一样回收。
  */

/**
  * 模式匹配取需要字段的userid做key
  */
object TagsUtils {
      //过滤需要的字段
      val OneUserId =
         """
           | imei != '' or mac != '' or openudid != '' or androidid != '' or idfa != '' or
           | imeimd5 != '' or macmd5  != '' or openudidmd5  != '' or androididmd5  != '' or idfamd5 != '' or
           | imeisha1 != '' or macsha1 != '' or openudidsha1 != '' or androididsha1 != '' or idfasha1 != ''
         """.stripMargin

      def getOneUserId(row:Row)= {

         def getOneUserId(row:Row):String= {

            row match {

               case v if StringUtils.isNotBlank(v.getAs[String]("imei")) => v.getAs[String]("imei")
               case v if StringUtils.isNotBlank(v.getAs[String]("mac")) => v.getAs[String]("mac")
               case v if StringUtils.isNotBlank(v.getAs[String]("openudid")) => v.getAs[String]("openudid")
               case v if StringUtils.isNotBlank(v.getAs[String]("imeandroididi")) => v.getAs[String]("androidid")
               case v if StringUtils.isNotBlank(v.getAs[String]("idfa")) => v.getAs[String]("idfa")
               case v if StringUtils.isNotBlank(v.getAs[String]("imeimd5")) => v.getAs[String]("imeimd5")
               case v if StringUtils.isNotBlank(v.getAs[String]("macmd5")) => v.getAs[String]("macmd5")
               case v if StringUtils.isNotBlank(v.getAs[String]("openudidmd5")) => v.getAs[String]("openudidmd5")
               case v if StringUtils.isNotBlank(v.getAs[String]("androididmd5")) => v.getAs[String]("androididmd5")
               case v if StringUtils.isNotBlank(v.getAs[String]("idfamd5")) => v.getAs[String]("idfamd5")
               case v if StringUtils.isNotBlank(v.getAs[String]("imeisha1")) => v.getAs[String]("imeisha1")
               case v if StringUtils.isNotBlank(v.getAs[String]("macsha1")) => v.getAs[String]("macsha1")
               case v if StringUtils.isNotBlank(v.getAs[String]("openudidsha1")) => v.getAs[String]("openudidsha1")
               case v if StringUtils.isNotBlank(v.getAs[String]("androididsha1")) => v.getAs[String]("androididsha1")
               case v if StringUtils.isNotBlank(v.getAs[String]("idfasha1")) => v.getAs[String]("idfasha1")
            }
         }
      }
}
