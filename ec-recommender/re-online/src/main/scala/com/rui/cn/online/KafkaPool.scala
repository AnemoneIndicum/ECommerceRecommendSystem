package com.rui.cn.online

import java.util.Properties

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

//包装Kafka客户端
class KafkaProxy(broker: String) {
  val props: Properties = new Properties();
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val kafkaClient = new KafkaProducer[String, String](props)

}

//创建一个创建KafkaProxy的工厂  commons-pool2

class KafkaProxyFactory(broker: String) extends BasePooledObjectFactory[KafkaProxy] {

  // 创建实例
  override def create(): KafkaProxy = new KafkaProxy(broker)

  // 包装实例
  override def wrap(t: KafkaProxy): PooledObject[KafkaProxy] = new DefaultPooledObject[KafkaProxy](t)
}


object KafkaPool {

  private var kafkaPool: GenericObjectPool[KafkaProxy] = null

  def apply(borker: String): GenericObjectPool[KafkaProxy] = {

    if (kafkaPool == null) {
      KafkaPool.synchronized {
        this.kafkaPool = new GenericObjectPool[KafkaProxy](new KafkaProxyFactory(borker))
      }
    }

    kafkaPool
  }

}
