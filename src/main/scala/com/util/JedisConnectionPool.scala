package com.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool {
    val config = new JedisPoolConfig()
    config.setMaxTotal(20)
    config.setMaxIdle(10)

    private val pool = new JedisPool(config,"mini01",6379,10000)

    def getConnection():Jedis={
      pool.getResource
    }
}
