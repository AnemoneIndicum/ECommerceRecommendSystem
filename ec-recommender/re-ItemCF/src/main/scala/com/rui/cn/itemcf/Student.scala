package com.rui.cn.itemcf

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Student {

  case class Student(name: String, age: Int, sex: String) extends Serializable

  def main(args: Array[String]): Unit = {
    // 创建Spark配置
    val sparkConf: SparkConf = new SparkConf().setAppName("MockData").setMaster("local[*]")

    // 创建Spark SQL 客户端
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    /*import spark.implicits._

   // 加载数据到Hive
   val studentRdd = spark.sparkContext.textFile("G:\\project\\hadoop\\ECommerceRecommendSystem\\ec-recommender\\re-ItemCF\\src\\main\\resources\\student.txt")
   val studentDF = studentRdd.map(_.split(",")).map(attr => Student(attr(0), attr(1).toInt, attr(2))).toDF("name", "age", "sex")
   studentDF.createOrReplaceTempView("student")

   val frame = spark.sql("select age,sex,count(1) from student group by age,sex")
   frame.show()
   spark.close*/

    val studentRDD: RDD[String] = spark.sparkContext.textFile("G:\\project\\hadoop\\ECommerceRecommendSystem\\ec-recommender\\re-ItemCF\\src\\main\\resources\\student.txt")
    studentRDD.map(line => {
      val attr = line.split(",")
      ((attr(1), attr(2)), 1)
    }).reduceByKey(_ + _).collect().foreach(println(_))
  }
}