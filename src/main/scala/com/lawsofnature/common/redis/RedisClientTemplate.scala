package com.lawsofnature.common.redis

import java.lang.Long
import java.util
import javax.inject.{Inject, Named}

import com.lawsofnature.common.helper.JsonHelper
import org.slf4j.LoggerFactory
import redis.clients.jedis._

/**
  * Created by fangzhongwei on 2016/11/23.
  */
trait RedisClientTemplate {
  def init

  def close

  def setString(key: String, value: String, expireSeconds: Int): Boolean

  def set(key: String, value: AnyRef, expireSeconds: Int): Boolean

  def get[T](key: String, c: Class[T]): Option[T]

  def getString(key: String): Option[String]

  def delete(key: String): Boolean
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
  val CHARSET = "UTF-8"
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

  override def set(key: String, value: AnyRef, expireSeconds: Int): Boolean = setString(key, JsonHelper.writeValueAsString(value), expireSeconds)

  override def setString(key: String, value: String, expireSeconds: Int): Boolean = {
    var shardedJedis: ShardedJedis = null
    try {
      assert(expireSeconds > 0, "expect expireSeconds > 0")
      val keyBytes: Array[Byte] = key.getBytes(CHARSET)
      val valueBytes: Array[Byte] = value.getBytes(CHARSET)
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

  override def get[T](key: String, c: Class[T]): Option[T] = {
    getStringFromCache(key) match {
      case Some(str) => Some(JsonHelper.readByType[T](str, c))
      case None => None
    }
  }

  override def getString(key: String): Option[String] = {
    getStringFromCache(key) match {
      case Some(str) => Some(str)
      case None => None
    }
  }

  def getStringFromCache(key: String): Option[String] = {
    var shardedJedis: ShardedJedis = null
    try {
      shardedJedis = getShardedJedis
      val pipel: ShardedJedisPipeline = shardedJedis.pipelined()
      val response: Response[Array[Byte]] = pipel.get(key.getBytes(CHARSET))
      pipel.sync()
      val bytes: Array[Byte] = response.get()
      if (bytes == null || bytes.length == 0) None else Some(new String(bytes, CHARSET))
    } finally {
      if (shardedJedis != null) shardedJedis.close()
    }
  }

  override def delete(key: String): Boolean = {
    var shardedJedis: ShardedJedis = null
    try {
      shardedJedis = getShardedJedis
      val pipel: ShardedJedisPipeline = shardedJedis.pipelined()
      val response: Response[Long] = pipel.del(key.getBytes(CHARSET))
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
}