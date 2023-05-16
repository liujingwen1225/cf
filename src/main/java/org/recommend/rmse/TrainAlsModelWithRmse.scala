package org.recommend.rmse

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.recommend.util.{MysqlUtil, SessionUtil}

/**
 * ALS协同过滤算法模型训练基于均方根误差进行验证模型准确度
 */
object TrainAlsModelWithRmse {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val session = SessionUtil.createSparkSession(this.getClass)
    val allData = UserRatings.getRatings(session)

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
      // 模型的最大迭代次数
      .setMaxIter(10)
      .setRegParam(0.1)
      .setUserCol("user_id")
      .setItemCol("item_id")
      .setRatingCol("rating")
    val model = als.fit(trainData)
    // 冷启动处理。nan或者drop
    model.setColdStartStrategy("drop")
    // rmse 模型预测
    val predictions = model.transform(testData)
    predictions.show()
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println("均方根误差(rmse)=" + rmse)
    session.close()
  }


}
