package com.jxjxgo.common.redis

import java.lang.Long
import java.nio.charset.StandardCharsets
import java.util
import javax.inject.{Inject, Named}

import org.slf4j.LoggerFactory
import redis.clients.jedis._

/**
  * Created by fangzhongwei on 2016/11/23.
  */
trait RedisClientTemplate {
  def init

  def close

  def zadd(key: Array[Byte], index: Double, bytes: Array[Byte])

  def setString(key: String, value: String, expireSeconds: Int): Boolean

  def setBytes(keyBytes: Array[Byte], valueBytes: Array[Byte], expireSeconds: Int): Boolean

  def getString(key: String): Option[String]

  def getBytes(keyBytes: Array[Byte]): Option[Array[Byte]]

  def delete(key: String): Boolean

  def deleteBytes(keyBytes: Array[Byte]): Boolean

  def expire(keyBytes: Array[Byte], expireSeconds: Int): Boolean

  def expire(key:String, expireSeconds: Int): Boolean
}

class RedisClientTemplateImpl @Inject()(@Named("redis.shards") cluster: String,
                                        @Named("redis.shard.connection.timeout") shardConnectionTimeout: Int,
                                        @Named("redis.min.idle") minIdle: Int,
                                        @Named("redis.max.idle") maxIdle: Int,
                                        @Named("redis.max.total") maxTotal: Int,
                                        @Named("redis.max.wait.millis") maxWaitMillis: Int,
                                        @Named("redis.test.on.borrow") testOnBorrow: Boolean) extends RedisClientTemplate {
  val logger = LoggerFactory.getLogger(this.getClass)

  val SUCCESS_TAG = "OK"
  val MAX_KEY_BYTES = 1024 * 10
  val MAX_VALUE_BYTES = 1024 * 50
  var shardedJedisPool: ShardedJedisPool = _

  def apply(cluster: String, shardConnectionTimeout: Int, minIdle: Int, maxIdle: Int, maxTotal: Int, maxWaitMillis: Int, testOnBorrow: Boolean): RedisClientTemplateImpl = new RedisClientTemplateImpl(cluster, shardConnectionTimeout, minIdle, maxIdle, maxTotal, maxWaitMillis, testOnBorrow)

  def getPoolConfig: JedisPoolConfig = {
    val config: JedisPoolConfig = new JedisPoolConfig()
    config.setMinIdle(minIdle)
    config.setMaxIdle(maxIdle)
    config.setMaxTotal(maxTotal)
    config.setMaxWaitMillis(maxWaitMillis)
    config.setTestOnBorrow(testOnBorrow)
    config
  }

  def getShards: util.List[JedisShardInfo] = {
    val shards: util.List[JedisShardInfo] = new util.ArrayList[JedisShardInfo]()
    var jedisShardInfo: JedisShardInfo = null
    cluster.split(",").foreach(s => {
      val hostAndPortArray: Array[String] = s.split(":")
      jedisShardInfo = new JedisShardInfo(hostAndPortArray(0), hostAndPortArray(1))
      jedisShardInfo.setConnectionTimeout(shardConnectionTimeout)
      shards.add(jedisShardInfo)
    })
    shards
  }

  override def init = shardedJedisPool = new ShardedJedisPool(getPoolConfig, getShards)

  def getShardedJedis: ShardedJedis = shardedJedisPool.getResource()

  override def close: Unit = shardedJedisPool.close()

  override def setString(key: String, value: String, expireSeconds: Int): Boolean = {
    setBytes(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8), expireSeconds)
  }

  override def setBytes(keyBytes: Array[Byte], valueBytes: Array[Byte], expireSeconds: Int): Boolean = {
    var shardedJedis: ShardedJedis = null
    try {
      assert(expireSeconds > 0, "expect expireSeconds > 0")
      assert(keyBytes.length < MAX_KEY_BYTES, "expect keyBytes < " + MAX_KEY_BYTES)
      assert(valueBytes.length < MAX_VALUE_BYTES, "expect valueBytes < " + MAX_VALUE_BYTES)
      shardedJedis = getShardedJedis
      val pipel: ShardedJedisPipeline = shardedJedis.pipelined()
      val response: Response[String] = pipel.set(keyBytes, valueBytes)
      if (expireSeconds > 0) pipel.expire(keyBytes, expireSeconds)
      pipel.sync()
      true
    } catch {
      case ex: Exception =>
        logger.error("cache", ex)
        false
    } finally {
      if (shardedJedis != null) shardedJedis.close()
    }
  }

  override def expire(keyBytes: Array[Byte], expireSeconds: Int): Boolean = {
    var shardedJedis: ShardedJedis = null
    try {
      assert(expireSeconds > 0, "expect expireSeconds > 0")
      assert(keyBytes.length < MAX_KEY_BYTES, "expect keyBytes < " + MAX_KEY_BYTES)
      shardedJedis = getShardedJedis
      val pipel: ShardedJedisPipeline = shardedJedis.pipelined()
      if (expireSeconds > 0) pipel.expire(keyBytes, expireSeconds)
      pipel.sync()
      true
    } catch {
      case ex: Exception =>
        logger.error("cache", ex)
        false
    } finally {
      if (shardedJedis != null) shardedJedis.close()
    }
  }

  override def getString(key: String): Option[String] = {
    getStringFromCache(key) match {
      case Some(str) => Some(str)
      case None => None
    }
  }

  def getStringFromCache(key: String): Option[String] = {
    getBytes(key.getBytes(StandardCharsets.UTF_8)) match {
      case Some(array) => Some(new String(array, StandardCharsets.UTF_8))
      case None => None
    }
  }

  override def getBytes(keyBytes: Array[Byte]): Option[Array[Byte]] = {
    var shardedJedis: ShardedJedis = null
    try {
      shardedJedis = getShardedJedis
      val pipel: ShardedJedisPipeline = shardedJedis.pipelined()
      val response: Response[Array[Byte]] = pipel.get(keyBytes)
      pipel.sync()
      val bytes: Array[Byte] = response.get()
      if (bytes == null || bytes.length == 0) None else Some(bytes)
    } finally {
      if (shardedJedis != null) shardedJedis.close()
    }
  }

  override def delete(key: String): Boolean = {
    deleteBytes(key.getBytes(StandardCharsets.UTF_8))
  }

  override def deleteBytes(keyBytes: Array[Byte]): Boolean = {
    var shardedJedis: ShardedJedis = null
    try {
      shardedJedis = getShardedJedis
      val pipel: ShardedJedisPipeline = shardedJedis.pipelined()
      val response: Response[Long] = pipel.del(keyBytes)
      pipel.sync()
      response.get() == 1
    } catch {
      case ex: Exception =>
        logger.error("cache", ex)
        false
    } finally {
      if (shardedJedis != null) shardedJedis.close()
    }
  }

  override def zadd(key: Array[Byte], index: Double, member: Array[Byte]): Unit = {
    var shardedJedis: ShardedJedis = null
    try {
      shardedJedis = getShardedJedis
      val pipel: ShardedJedisPipeline = shardedJedis.pipelined()
      val response: Response[Long] = pipel.zadd(key, index, member)
      pipel.sync()
      response.get() == 1
    } catch {
      case ex: Exception =>
        logger.error("cache", ex)
        false
    } finally {
      if (shardedJedis != null) shardedJedis.close()
    }
  }

  override def expire(key: String, expireSeconds: Int): Boolean = {
    expire(key.getBytes(StandardCharsets.UTF_8), expireSeconds)
  }

  init
}