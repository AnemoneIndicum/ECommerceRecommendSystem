package com.rui.cn.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

/**
  * 基于类容的推荐
  *
  * @author zhangrl
  * @time 2019/11/26-17:13
  **/
object ContentRecommend {
  // 定义mongodb中存储的表名
  val MONGODB_PRODUCT_COLLECTION = "Product"
  // 类容推荐 保存
  val CONTENT_PRODUCT_RECS = "ContentBasedProductRecs"


  def main(args: Array[String]): Unit = {
    val config: Map[String, String] = Map(
      "spark.cores" -> "local[*]",
      "mongo.url" -> "mongodb://hdp-00:27017/product",
      "mongo.db" -> "product"
    )

    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(config("spark.cores"))

    // sparkSession
    val sparkSession: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    import sparkSession.implicits._
    // 定义隐式对象
    implicit val mongoConfig = MongoConfig(config("mongo.url"), config("mongo.db"))
    val productDF: DataFrame = OperateMongoDb.loadInfoAsDataFrame(sparkSession, MONGODB_PRODUCT_COLLECTION).as[Product].map {
      x =>
        (x.productId, x.name, x.tags.map(c => if (c == '|') ' ' else c))
    }.toDF("productId", "name", "tags")
      .cache()

    // TODO: 用TF-IDF提取商品特征向量
    // 实例化分词器 用来做分词 默认是按照空格分
    val tokenizer: Tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")
    // 用分词器转换，得到一个基于 tags 的新列 words
    val wordsDataDF: DataFrame = tokenizer.transform(productDF)

    // 定义hashingTf工具 计算词频TF
    val hashingTF: HashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(800)
    val featureDf: DataFrame = hashingTF.transform(wordsDataDF)

    // 定义idf工具 计算tf-idf 逆文档频率IDF   TF-IDF = TF * IDF
    val idf: IDF = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val iDFModel: IDFModel = idf.fit(featureDf)

    val rescaledDataDF: DataFrame = iDFModel.transform(featureDf)
    // 对数据进行转换，得到RDD形式的features
    val productFeatures: RDD[(Int, DoubleMatrix)] = rescaledDataDF.map {
      row => (row.getAs[Int]("productId"), row.getAs[SparseVector]("features").toArray)
    }
      .rdd
      .map {
        case (productId, features) => (productId, new DoubleMatrix(features))
      }

    // 两两配对计算余弦相似度(自己跟自己迪卡集)
    val productResDF: DataFrame = productFeatures.cartesian(productFeatures)
      .filter { // 排除相同的商品
        case (a, b) => a._1 != b._1
      } // 计算余弦相似度
      .map {
      case (a, b) =>
        val simScore = cosineSimilarity(a._2, b._2)
        (a._1, (b._1, simScore))
    }
      .filter(_._2._2 > 0.4)
      .groupByKey()
      .map {
        case (productId, resc) =>
          RecommendationProduct(productId, resc.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()
        productResDF.show()
    // 保存数据库
    OperateMongoDb.dataFrameToSave(productResDF, CONTENT_PRODUCT_RECS)

    productDF.unpersist()
    sparkSession.stop()
  }

  /**
    * 余弦相似度
    *
    * @param vec1
    * @param vec2
    * @return
    */
  private def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }
}
