package com.rui.cn.dataload

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.sql.DataFrame

/**
 * 操作数据库的操作
 *
 * @author zhangrl
 * @time 2019/11/21-10:27
 **/
object CreateDataRecord {
  // Product在MongoDB中的Collection名称【表】
  val PRODUCTS_COLLECTION_NAME = "Product"

  // Rating在MongoDB中的Collection名称【表】
  val RATINGS_COLLECTION_NAME = "Rating"


  /**
    * Store Data In MongoDB
    *
    * @param products    商品数据集
    * @param ratings     评分数据集
    * @param mongoConfig MongoDB的配置
    */
  def storeDataInMongo(products: DataFrame, ratings: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // 建立连接
    val mongoClient: MongoClient = MongoClient(MongoClientURI(mongoConfig.url))
    // 删除products的Collection
    mongoClient(mongoConfig.dbName)(PRODUCTS_COLLECTION_NAME).dropCollection()
    // 删除Rating的Collection
    mongoClient(mongoConfig.dbName)(RATINGS_COLLECTION_NAME).dropCollection()

    // 写入products信息
    products.write
      .option("uri", mongoConfig.url)
      .option("collection", PRODUCTS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //将Rating数据集写入到MongoDB
    ratings
      .write.option("uri", mongoConfig.url)
      .option("collection", RATINGS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 建立索引信息
    mongoClient(mongoConfig.dbName)(PRODUCTS_COLLECTION_NAME).createIndex(MongoDBObject("productId" -> 1))
    mongoClient(mongoConfig.dbName)(RATINGS_COLLECTION_NAME).createIndex(MongoDBObject("productId" -> 1))
    mongoClient(mongoConfig.dbName)(RATINGS_COLLECTION_NAME).createIndex(MongoDBObject("userId" -> 1))

    mongoClient.close()
  }
}
