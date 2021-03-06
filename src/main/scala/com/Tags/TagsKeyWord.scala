package com.Tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsKeyWord {
      /**
        * 打标签的统一接口
        */
      override def makeTags(args: Any*): List[(String, Int)] = {
         var list = List[(String,Int)]()

         val row = args(0).asInstanceOf[Row]
         val stopword = args(1).asInstanceOf[ Broadcast[collection.Map[String, Int]]]

         val  keywords = row.getAs[String]("keywords").split("\\|")

         keywords.filter(word=>{
            word.size>3 && word.size<= 8 && !word.contains(stopword)
         })
            .foreach(word=>list:+=("k"+word,1))

         list
      }
}
