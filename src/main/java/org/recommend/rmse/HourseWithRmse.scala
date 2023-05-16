package org.recommend.rmse

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.recommend.util.{HourseMysqlUtil, MysqlUtil, SessionUtil}


/**
 * 基于物品的协同过滤算法的均方根误差rmse 值计算
 * item cf
 */
object HourseWithRmse {

  def main(args: Array[String]): Unit = {
    val session = SessionUtil.createSparkSession(this.getClass)
    val recommend = HourseMysqlUtil.readMysqlTable(session, "recommend")
    val www = HourseMysqlUtil.readMysqlTable(session, "www")
    val frame = recommend.join(www, Seq("user_id", "hourse_id"))
      .selectExpr("user_id", "hourse_id", "cast(rating as double) rating", "cast(zonhe as double) zonhe")
    frame.show()
    // 计算rmse
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("zonhe")
    val rmse = evaluator.evaluate(frame)
    println("均方根误差(rmse)=" + rmse)
  }
}
