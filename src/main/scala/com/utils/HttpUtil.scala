package com.utils

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

object HttpUtil {
   def get(url:String):String ={
      val client = HttpClients.createDefault()
      val get = new HttpGet(url)

      //请求
      val response:CloseableHttpResponse = client.execute(get)

      // 获取返回结果
      EntityUtils.toString(response.getEntity,"UTF-8")

   }

}
