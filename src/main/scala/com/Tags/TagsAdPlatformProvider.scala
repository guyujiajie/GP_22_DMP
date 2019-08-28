package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object TagsAdPlatformProvider extends Tag{

   override def makeTags(args: Any*): List[(String, Int)] = {

      var list = List[(String,Int)]()

      val row = args(0).asInstanceOf[Row]

      val adPlatformProviderId = row.getAs[Int]("adplatformproviderid")

      list :+= ("CN" + adPlatformProviderId, 1)
      list

   }

}
