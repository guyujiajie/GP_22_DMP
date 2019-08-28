package com.Tags

import com.utils.Tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row


/**
  * 广告类型标签
  */
object TagsAd extends Tag{
   /**
     * 打标签的统一接口
     */

   override def makeTags(args: Any*): List[(String, Int)] = {
      var list: List[(String, Int)] = List[(String,Int)]()

      //解析参数转成了any 再转换回来，不需要判断，直接强转
      val row = args(0).asInstanceOf[Row]
      //获取广告类型，广告类型名称
      val adType = row.getAs[Int]("adspacetype")
      //小于10，前面加0
      adType match {
         case v if v > 9 => list :+= ("LC" + v, 1)
         case v if v <= 9 && v > 0 => list :+= ("LC0" + v ,1)
      }
      val adName = row.getAs[String]("adspacetypename")
      if (StringUtils.isNotBlank(adName)) {
         list :+= ("LN" + adName, 1)
      }
      list
   }
}
