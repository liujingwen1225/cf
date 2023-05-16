package org.recommend.rmse

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.recommend.util.SessionUtil


/**
 * 基于物品的协同过滤算法的均方根误差rmse 值计算
 * item cf
 */
object ItemCfWithRmse {

  def main(args: Array[String]): Unit = {
    val session = SessionUtil.createSparkSession(this.getClass)

    import org.apache.spark.sql.functions._
    import session.implicits._
    // 获取评分矩阵
    val df = UserRatings.getRatings(session)
    val df_sim = df.selectExpr("user_id ", "item_id as item_v", "rating as rating_v")
    val df_dot = df.join(df_sim, "user_id")
      .filter("item_id <>  item_v")
      .selectExpr("user_id", "item_id", "item_v", " cast(rating as double) * cast( rating_v as double) as rating")
      .groupBy("item_id", "item_v")
      .agg(sum("rating").as("rating"))

    val df_end = df.selectExpr("user_id", "item_id", " cast(rating as double) *  cast(rating as double)as rating")
      .groupBy("item_id")
      .agg(sum("rating").as("rating"))
      .selectExpr("item_id", "sqrt(rating) as rating_s")

    val df_end_sim = df_end.selectExpr("item_id as item_v", "rating_s as rating_v")
    // 存储物品之间的相似度
    val df_sim_rel = df_dot.join(df_end, "item_id")
      .join(df_end_sim, "item_v")
      .selectExpr("item_id", "item_v", "rating /(rating_s * rating_v)  as cos")

    //根据物品之间的相似度来求出 需要推荐的物品
    val df_topK = df_sim_rel
      .selectExpr("item_id", "item_v", "cos", "row_number() over(partition by item_id order by cos desc) as rank")
      .where("rank <=5")
    // 获取相似的物品的集合
    val df_list = df_topK.rdd.map(row => {
      (row(0).toString, (row(1).toString, row(2).toString))
    })
      .groupByKey()
      .mapValues(v => {
        v.toArray.map(l => {
          l._1 + "_" + l._2
        })
      }).toDF("item_id", "item_list")
    // 获取用户所选的课程列表
    val df_u = df.rdd.map(row => {
      (row(0).toString, (row(1).toString, row(2).toString))
    }).groupByKey().mapValues(v => {
      v.toArray.map(l => l._1 + "_" + l._2)
    }).toDF("user_id", "item_l")
    // 用户 物品id 物品列表
    val df_u_item = df.join(df_u, "user_id")
      .selectExpr("user_id", "item_id", "rating", "item_l")

    // 进行评分预测 prediction
    val frame = df_u_item.join(df_list, "item_id")
      .selectExpr("user_id", "item_id", "rating", "explode(item_list) rec")
      .selectExpr("user_id", "item_id", "rating", "cast(split(rec,'_')[1] as DOUBLE)*rating prediction")
      .groupBy("user_id", "item_id", "rating")
      .sum("prediction")
      .withColumnRenamed("sum(prediction)", "prediction")
    frame.show()

    // 计算rmse
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(frame)
    println("均方根误差(rmse)=" + rmse)
  }
}
