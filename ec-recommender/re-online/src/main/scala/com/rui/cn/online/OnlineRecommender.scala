package com.rui.cn.online

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 用户商品实时推荐
  *
  * @author zhangrl
  * @time 2019/11/25-16:29
  **/
object OnlineRecommender {
  // 用户商品实时推荐结果保存
  val STREAM_RECS = "StreamRecs"
  // 向平相似度推荐结果
  val PRODUCT_RECS = "ProductRecs"

  val MAX_USER_RATING_NUM = 20
  val MAX_SIM_PRODUCTS_NUM = 20

  def main(args: Array[String]): Unit = {
    val config: Map[String, String] = Map(
      "spark.cores" -> "local[*]",
      "mongo.url" -> "mongodb://hdp-00:27017/product",
      "mongo.db" -> "product"
    )

    // 创建sparkConfig
    val sparkConf: SparkConf = new SparkConf()
      .setMaster(config("spark.cores"))
      .setAppName(this.getClass.getSimpleName)

    // 创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    import sparkSession.implicits._
    val sparkContext = sparkSession.sparkContext
    // StreamingContext
    val streamingContext: StreamingContext = new StreamingContext(sparkContext, Seconds(5))

    /** *********************************************************************************************************/
    // 定义隐式对象
    implicit val mongoConfig = MongoConfig(config("mongo.url"), config("mongo.db"))
    // 家在数据并广播  Map[Int, Map[Int,Double]]
    val productRecs = OperateMongoDb.loadInfoAsDataFrame(sparkSession, PRODUCT_RECS).as[RecommendationProduct].rdd.map {
      arr =>
        (arr.productId, arr.recs.map(x => (x.productId, x.score)).toMap)
    }.collectAsMap()
    // 使用广播变量
    val productBoradcast: Broadcast[collection.Map[Int, Map[Int, Double]]] = sparkContext.broadcast(productRecs)


    /** *********************************************************************************************************/
    // 卡夫卡消费者参数配置
    val kafkaPro: Map[String, String] = Map[String, String](
      //用于初始化链接到集群的地址
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertyUtil.getProperty("bootstrap.servers"),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      //用于标识这个消费者属于哪个消费团体
      ConsumerConfig.GROUP_ID_CONFIG -> PropertyUtil.getProperty("product.group.id"),
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性  112|12321|4.565|1564754545
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )

    // 产生消费者数据流
    val stream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaPro, Set(PropertyUtil.getProperty("product.kafka.topics")))
    // 对kafkaStream进行处理，产生评分流，userId|productId|score|timestamp
    val ratingStream: DStream[(Int, Int, Double, Int)] = stream.map {
      msg =>
        val item = msg._2.split("\\|")
        (item(0).toInt, item(1).toInt, item(2).toDouble, item(3).toInt)
    }
    ratingStream.foreachRDD { rdd =>
      rdd.map { case (userId, productId, score, timestamp) => println(s"==================>$userId+$productId+$score+$timestamp")
        // 获取当前用户最近的M次商品评分 uid mid:score mid:score mid:score  =========>(mid,score)
        val userRecentlyRatings: Array[(Int, Double)] = Commons.getUserRecentlyRating(MAX_USER_RATING_NUM, userId)

        // 获取商品P最相似的且当前用户未看过的K个商品
        val simMovies: Array[Int] = Commons.getTopSimProducts(MAX_SIM_PRODUCTS_NUM, productId, userId, productBoradcast.value)

        // 计算待选商品的推荐优先级
        val streamRecs = Commons.computeProductScores(productBoradcast.value, userRecentlyRatings, simMovies)

        //将数据保存到MongoDB
        OperateMongoDb.saveRecsToMongoDB(userId, streamRecs)
      }.count()
    }
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
