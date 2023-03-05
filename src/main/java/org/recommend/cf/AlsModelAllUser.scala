package org.recommend.cf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.recommend.util.{MysqlUtil, SessionUtil}

/**
 * ALS协同过滤算法模型训练
 */
object AlsModelAllUser {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val session = SessionUtil.createSparkSession(this.getClass)

    //注册 mysql 用户评分表
    MysqlUtil.readMysqlTable(session, "student_course")
    MysqlUtil.readMysqlTable(session, "course")
    // 用户评分表，用于训练模型
    val GetUserRatingSql =
      s"""
         |select st.student_id, st.course_id, (ifnull(st.rating, 2) * 0.4 + c.grading * 0.6) rating
         |from student_course st
         |         left join course c on c.id = st.course_id
         |where grading is not null
      """.stripMargin
    println("加载评分表")
    val allData = session.sql(GetUserRatingSql)
    // 样本基本信息为
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
      //      .setImplicitPress(true)
      // 模型的最大迭代次数（默认10）
      .setMaxIter(10)
      .setRegParam(0.1)
      // 隐性反馈，这个参数决定了偏好行为强度的基准
      //      .setAlpha(1)
      .setUserCol("student_id")
      .setItemCol("course_id")
      .setRatingCol("rating")
    val model = als.fit(trainData)
    // 冷启动处理。nan或者drop
    model.setColdStartStrategy("drop")
    val frame = model.recommendForAllUsers(20)
    // todo 过滤
    frame.show(false)
    session.close()
  }

}
