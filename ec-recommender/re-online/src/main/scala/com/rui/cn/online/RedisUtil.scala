package com.rui.cn.online

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * redis工具类
  *
  * @author zhangrl
  * @time 2019/7/22-15:02
  **/
object RedisUtil extends Serializable {

  val host = "10.10.30.11"
  val port = 6381
  val database = 0
  val timeout = 30000
  val password = "hzed2018"
  val config = new JedisPoolConfig
  config.setMaxTotal(100)
  config.setMaxIdle(50)
  config.setMinIdle(10)
  //设置连接时的最大等待毫秒数
  config.setMaxWaitMillis(10000)
  //设置在获取连接时，是否检查连接的有效性
  config.setTestOnBorrow(true)
  //设置释放连接到池中时是否检查有效性
  config.setTestOnReturn(true)

  //在连接空闲时，是否检查连接有效性
  config.setTestWhileIdle(true)

  //两次扫描之间的时间间隔毫秒数
  config.setTimeBetweenEvictionRunsMillis(30000)
  //每次扫描的最多的对象数
  config.setNumTestsPerEvictionRun(10)
  //逐出连接的最小空闲时间，默认是180000（30分钟）
  config.setMinEvictableIdleTimeMillis(60000)
  lazy val pool: JedisPool = new JedisPool(config,host,port,timeout,password,database)
  //释放资源
  lazy val hook = new Thread{
    override def run() = {
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook)

  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://127.0.0.1:27017/product"))

}
