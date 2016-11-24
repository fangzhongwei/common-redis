package com.lawsofnature.common.redis

import java.util
import javax.inject.{Inject, Named}

import redis.clients.jedis._

/**
  * Created by fangzhongwei on 2016/11/23.
  */
class RedisClientTemplate @Inject()(@Named("redis.host") host:String,
                                    @Named("redis.port") port:Int) {

}

object Test extends App{
  private val info: JedisShardInfo = new JedisShardInfo("192.168.181.131")
  info.setConnectionTimeout(10000)
  private val shards: util.List[JedisShardInfo] = util.Arrays.asList(
    info)

  private val config: JedisPoolConfig = new JedisPoolConfig()
  config.setMaxIdle(100)
  config.setMaxWaitMillis(100000)
  config.setTestOnBorrow(true)
  config.setMaxTotal(100)
  config.setMinIdle(1)

  var pool: ShardedJedisPool  = new ShardedJedisPool(config, shards)

  var one: ShardedJedis  = pool.getResource()

  var pipeline: ShardedJedisPipeline  = one.pipelined()

  var start: Long  = System.currentTimeMillis()
  for (i <- 1 to 100000) {
    pipeline.set("sp" + i, "n" + i)
  }
  private val results: util.List[AnyRef] = pipeline.syncAndReturnAll()
  var end:Long  = System.currentTimeMillis()
  pool.returnResource(one)
  System.out.println("Pipelined@Pool SET: " + ((end - start)/1000.0) + " seconds")
  pool.destroy()

}