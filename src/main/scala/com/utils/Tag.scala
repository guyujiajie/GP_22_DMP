package com.utils

trait Tag {
   /**
     * 打标签的同一接口
     */

   //可以匹配多个参数 * 不限制参数类型，不一定为row
   def makeTags(args: Any*):List[(String,Int)]
}
