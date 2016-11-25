package com.lawsofnature.common.redis

import java.lang.Long
import java.util
import javax.inject.{Inject, Named}

import com.lawsofnature.common.helper.JsonHelper
import org.slf4j.LoggerFactory
import redis.clients.jedis._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

/**
  * Created by fangzhongwei on 2016/11/23.
  */
trait RedisClientTemplate {
  def init

  def close

  def setString(key: String, value: String, expireSeconds: Int): Future[Option[Boolean]]

  def set(key: String, value: AnyRef, expireSeconds: Int): Future[Option[Boolean]]

  def get[T](key: String, c: Class[T]): Future[Option[T]]

  def getString(key: String): Future[Option[String]]

  def delete(key: String): Future[Option[Boolean]]
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

  override def set(key: String, value: AnyRef, expireSeconds: Int): Future[Option[Boolean]] = setString(key, JsonHelper.writeValueAsString(value), expireSeconds)

  override def setString(key: String, value: String, expireSeconds: Int): Future[Option[Boolean]] = {
    var shardedJedis: ShardedJedis = null
    val promise: Promise[Option[Boolean]] = Promise[Option[Boolean]]()
    try {
      Future {
        assert(expireSeconds > 0, "expect expireSeconds > 0")
        val keyBytes: Array[Byte] = key.getBytes(CHARSET)
        val valueBytes: Array[Byte] = value.getBytes(CHARSET)
        assert(keyBytes.length < MAX_KEY_BYTES, "expect keyBytes < " + MAX_KEY_BYTES)
        assert(valueBytes.length < MAX_VALUE_BYTES, "expect valueBytes < " + MAX_VALUE_BYTES)
        shardedJedis = getShardedJedis
        val pipel: ShardedJedisPipeline = shardedJedis.pipelined()
        val response: Response[String] = pipel.set(keyBytes, valueBytes)
        pipel.expire(keyBytes, expireSeconds)
        pipel.sync()
        promise.success(Some(SUCCESS_TAG.equals(response.get())))
      }
    } catch {
      case ex: Exception => promise.failure(ex)
    } finally {
      if (shardedJedis != null) shardedJedis.close()
    }
    promise.future
  }

  override def get[T](key: String, c: Class[T]): Future[Option[T]] = {
    val promise: Promise[Option[T]] = Promise[Option[T]]()
    Future {
      getStringFromCache(key) match {
        case Some(str) => Some(JsonHelper.read[T](str, c))
        case None => promise.success(None)
      }
    }
    promise.future
  }

  override def getString(key: String): Future[Option[String]] = {
    val promise: Promise[Option[String]] = Promise[Option[String]]()
    Future {
      getStringFromCache(key) match {
        case Some(str) => promise.success(Some(str))
        case None => promise.success(None)
      }
    }
    promise.future
  }

  def getStringFromCache(key: String): Option[String] = {
    var shardedJedis: ShardedJedis = null
    try {
      shardedJedis = getShardedJedis
      val pipel: ShardedJedisPipeline = shardedJedis.pipelined()
      val response: Response[Array[Byte]] = pipel.get(key.getBytes(CHARSET))
      pipel.sync()
      val bytes: Array[Byte] = response.get()
      bytes.length match {
        case 0 => None
        case _ => Some(new String(bytes, CHARSET))
      }
    } finally {
      if (shardedJedis != null) shardedJedis.close()
    }
  }

  override def delete(key: String): Future[Option[Boolean]] = {
    var shardedJedis: ShardedJedis = null
    val promise: Promise[Option[Boolean]] = Promise[Option[Boolean]]()
    try {
      Future {
        shardedJedis = getShardedJedis
        val pipel: ShardedJedisPipeline = shardedJedis.pipelined()
        val response: Response[Long] = pipel.del(key.getBytes(CHARSET))
        pipel.sync()
        promise.success(Some(response.get() == 1))
      }
    } catch {
      case ex: Exception => promise.failure(ex)
    } finally {
      if (shardedJedis != null) shardedJedis.close()
    }
    promise.future
  }
}