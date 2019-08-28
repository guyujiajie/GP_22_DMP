package com.utils

/**
  * 数据类型转换
  * try{}catch{} 当try里面的出现异常catch抓取异常执行，try报错后面不执行。
  * 这里用到了模式匹配
  */
object Utils2Type {

  // String转换Int
  def toInt(str:String) :Int={
    try {
      str.toInt
    }catch {
      case _ :Exception => 0
    }
  }
  // String转换Double
  def toDouble(str:String) :Double={
    try {
      str.toDouble
    }catch {
      case _ :Exception => 0.0
    }
  }
}
