package org.recommend.cf

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

    //注册 mysql 用户评分表
    MysqlUtil.readMysqlTable(session, "student_course")
    MysqlUtil.readMysqlTable(session, "course")
    MysqlUtil.readMysqlTable(session, "sys_user")
    // 用户评分表，用于训练模型
    val GetUserRatingSql =
      s"""
         |select student_id,
         |       course_id,
         |       (1 + student_rating + course_ating + typera_ting + school_rating + labels_rating) as rating
         |from (select st.student_id,
         |             st.course_id,
         |             st.rating,
         |             c.grading,
         |             su.course_type,
         |             c.type,
         |             su.school_names,
         |             c.school,
         |             su.labels,
         |             c.labels,
         |             (ifnull(st.rating, 4) * 0.2)                     student_rating,
         |             (ifnull(c.grading, 3) * 0.3)                     course_ating,
         |             if(instr(su.course_type, c.type) > 0, 0.5, 0)    typera_ting,
         |             if(instr(su.school_names, c.school) > 0.5, 0, 0) school_rating,
         |             if(instr(su.labels, c.labels) > 0, 0.5, 0)       labels_rating
         |      from student_course st
         |               left join course c on c.id = st.course_id
         |               left join sys_user su on st.student_id = su.id) source
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
      .setUserCol("student_id")
      .setItemCol("course_id")
      .setRatingCol("rating")
//    val ranks = Array(10, 15, 20)
    val lambdas = Array(0.1, 0.01, 0.001)
    val result = for (lambda <- lambdas)
      yield {
        als
//          .setRank(rank)
          .setRegParam(lambda)
        val model = als.fit(trainData)
        // 冷启动处理。nan或者drop
        model.setColdStartStrategy("drop")
        // rmse 模型预测
        val predictions = model.transform(testData)
        println("正则化参数%s预测模型结果输出", lambda)
        predictions.show(true)
        val evaluator = new RegressionEvaluator()
          .setMetricName("rmse")
          .setLabelCol("rating")
          .setPredictionCol("prediction")
        val rmse = evaluator.evaluate(predictions)
//        (rank,lambda, rmse)
        (lambda, rmse)
      }
    println("结束训练模型")
    val rmse = session.createDataFrame(result).toDF("lambda", "rmse")
    println("数据均方根误差的结果。选取最rmse最小的最为算法参数")
    rmse.show()
    session.close()
  }


}
