package org.recommend.cf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SaveMode
import org.recommend.util.SessionUtil

/**
 * ALS协同过滤算法模型训练
 */
object AlsModel {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val session = SessionUtil.createSparkSession(this.getClass)

    val url = ""
    val user = "root"
    val password = "password"
    val table = "student_course"

    val prop = new java.util.Properties
    prop.setProperty("user", user)
    prop.setProperty("password", password)
    //注册 mysql 用户评分表
//    session.read.jdbc(url, table, prop).createOrReplaceTempView(table)
    session.
      read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("src/main/resources/data/rating.csv").createOrReplaceTempView("rating")
    // 用户评分表，用于训练模型
    val GetUserRatingSql =
      s"""
         |SELECT cast(user_id as int) user_id,
         |       cast(item_id as int) item_id,
         |       cast(rating as double) rating
         |FROM
         |  rating
      """.stripMargin
    println("加载评分表")
    val allData = session.sql(GetUserRatingSql)

    // 拆分训练集和测试集 后续验证最优参数
    val Array(trainData, testData) = allData.randomSplit(Array(0.7, 0.3))

    // 缓存
    trainData.persist()
    testData.persist()

    println("开始训练模型")

    // 使用ALS算法训练隐语义模型
    val als = new ALS()
      .setImplicitPrefs(true)
      // 模型的最大迭代次数（默认10）
      .setMaxIter(10)
      // 隐性反馈，这个参数决定了偏好行为强度的基准
      .setAlpha(1)
      .setUserCol("user_id")
      .setItemCol("item_id")
      .setRatingCol("rating")
    val ranks = Array(10, 15, 20)
    val lambdas = Array(0.1, 0.01, 0.001)
    val result = for (rank <- ranks; lambda <- lambdas)
      yield {
        als.setRank(rank)
          .setRegParam(lambda)
        val model = als.fit(trainData)
        // 冷启动处理。nan或者drop
        model.setColdStartStrategy("drop")
        // rmse 模型预测
        val predictions = model.transform(testData)
        val evaluator = new RegressionEvaluator()
          .setMetricName("rmse")
          .setLabelCol("rating")
          .setPredictionCol("prediction")
        val rmse = evaluator.evaluate(predictions)
        (rank, lambda, rmse)
      }
    println("结束训练模型")
    val rmse = session.createDataFrame(result).toDF("rank", "lambda", "rmse")
    rmse.show()
//    rmse.write.mode(SaveMode.Overwrite).saveAsTable("als_model_rmse")
    session.close()
  }


}
