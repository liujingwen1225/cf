package org.recommend.cf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.recommend.util.SessionUtil

/**
 * ALS协同过滤算法模型训练基于均方根误差进行验证模型准确度
 */
object TrainAlsModelWithRmse {

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

    val numRatings = allData.count()
    val numUser = allData.rdd.map(rating => rating.get(0)).distinct().count()
    val numItems = allData.rdd.map(rating => rating.get(1)).distinct().count()

    println("样本基本信息为：")
    println("样本数：" + numRatings)
    println("用户数：" + numUser)
    println("课程数：" + numItems)

    // 拆分训练集和测试集 后续验证最优参数
    val Array(trainData, testData) = allData.randomSplit(Array(0.7, 0.3))

    // 缓存
    trainData.persist()
    testData.persist()

    val trainDataNum = trainData.count()
    val testDataNum = testData.count()

    println("验证样本基本信息为：")
    println("训练样本数：" + trainDataNum)
    println("测试样本数：" + testDataNum)

    println("开始训练模型")

    // 使用ALS算法训练隐语义模型
    val als = new ALS()
    // 是否开启隐性反馈
//      .setImplicitPrefs(true)
      // 模型的最大迭代次数（默认10）
      .setMaxIter(10)
      // 隐性反馈，这个参数决定了偏好行为强度的基准
//      .setAlpha(1)
      .setUserCol("user_id")
      .setItemCol("item_id")
      .setRatingCol("rating")
    //    val ranks = Array(3, 4, 5)
    val lambdas = Array(0.1, 0.01, 0.001)
    val result = for (lambda <- lambdas)
      yield {
        als
//          .setRank(ranks)
          .setRegParam(lambda)
        val model = als.fit(trainData)
        // 冷启动处理。nan或者drop
        model.setColdStartStrategy("drop")
        // rmse 模型预测
        val predictions = model.transform(testData)
        println("正则化参数%s预测模型结果输出",lambda)
        predictions.show(true)
        val evaluator = new RegressionEvaluator()
          .setMetricName("rmse")
          .setLabelCol("rating")
          .setPredictionCol("prediction")
        val rmse = evaluator.evaluate(predictions)
        (lambda, rmse)
      }
    println("结束训练模型")
    val rmse = session.createDataFrame(result).toDF("lambda", "rmse")
    println("数据均方根误差的结果。选取最rmse最小的最为算法参数")
    rmse.show()
    session.close()
  }


}
