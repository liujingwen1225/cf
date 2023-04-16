package org.recommend.cf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.explode
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
    val UserRecommend = model.recommendForAllUsers(20)
    UserRecommend.show(20,false)
    // 过滤 已经选过了的课程
    UserRecommend.select(UserRecommend("student_id"), explode(UserRecommend("recommendations")))
      .toDF("student_id", "recommendations")
      .selectExpr("student_id", "recommendations.course_id", "recommendations.rating")
      .createOrReplaceTempView("als_result")
    // 过滤用户已经看过的节目和不在设备表的用户
    val sql =
      s"""
         |    SELECT result.student_id as user_id,
         |           result.course_id as course_id,
         |           row_number() over(partition by student_id order by rating desc) as ranking
         |    FROM als_result result
         |WHERE NOT EXISTS
         |    (SELECT 1
         |     FROM student_course sc
         |     WHERE result.student_id = sc.student_id
         |       AND result.course_id = sc.course_id)
        """.stripMargin
    val frame = session.sql(sql)
    MysqlUtil.writeMysqlTable(SaveMode.Overwrite,frame,"course_recommend")
    session.close()
  }

}
