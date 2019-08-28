package com.ZhouKao

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

object BusinessArea {

   def getData(string: String): String = {


      val jsonparse = JSON.parseObject( string )
      // val json = JSON.parseObject(string)
      val status = jsonparse.getIntValue( "status")
      if (status == 0) return ""
      val regeocodeJson: JSONObject = jsonparse.getJSONObject( "regeocode" )
      if (regeocodeJson == null && regeocodeJson.isEmpty) return ""

      val poisArray: JSONArray = regeocodeJson.getJSONArray( "pois" )
      if (poisArray == null && poisArray.isEmpty) return null

      val buffer = collection.mutable.ListBuffer[String]()

      for(itme<-poisArray.toArray()){
         val json = itme.asInstanceOf[JSONObject]
         buffer.append( json.getString( "businessarea" ))
      }
      buffer.mkString(",")
   }
}
