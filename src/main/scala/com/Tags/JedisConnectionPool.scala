package com.Tags

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * 连接池
  */
object JedisConnectionPool {

  val jedisConf = new JedisPoolConfig

  jedisConf.setMaxTotal(30)// 最大连接

  jedisConf.setMaxIdle(10)// 最大空闲

  val pool = new JedisPool(jedisConf,"node")
  // 获取Jedis对象
  def getConnection():Jedis={
    pool.getResource
  }
}
