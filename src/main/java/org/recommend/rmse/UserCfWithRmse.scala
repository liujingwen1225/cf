package org.recommend.rmse

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.recommend.util.SessionUtil

/**
 * 基于用户的协同过滤算法的均方根误差rmse 值计算
 * user cf
 */
object UserCfWithRmse {
  def main(args: Array[String]): Unit = {
    val session = SessionUtil.createSparkSession(this.getClass)
    import org.apache.spark.sql.functions._
    import session.implicits._

    val df = UserRatings.getRatings(session)
    val df_sim = df.selectExpr("user_id as user_v", "item_id as item_id", "rating as rating_v")

    // 建立item-user倒排表,点乘的和
    val df_join = df.join(df_sim, "item_id").filter("cast(user_id as long) <> cast(user_v as long)")
    val df_dot = df_join.selectExpr("user_id", "user_v", "cast(rating as long)  * cast(rating_v as long) as rating_sum")
      .groupBy("user_id", "user_v")
      .agg(sum("rating_sum").as("rating_s"))

    // 计算每个用户的模的和
    val df_score_tmp = df.selectExpr("user_id", "cast(rating as long) * cast(rating as long) as rating_s")
      .groupBy("user_id")
      .agg(sum("rating_s").as("rating_sum"))
    val df_score = df_score_tmp.selectExpr("user_id as user_c", "sqrt(rating_sum) as rating_sum")

    // 计算求出不同用户之间的相似度
    val df_sim_tmp = df_dot.join(df_score, df_dot("user_id") === df_score("user_c"))
      .selectExpr("user_id", "user_v", "rating_s", "rating_sum as rating_a")
    val df_sim_rel = df_sim_tmp.join(df_score, df_sim_tmp("user_v") === df_score("user_c"))
      .selectExpr("user_id", "user_v", "rating_s/(rating_a*rating_sum)  as cos")

    // 获得最大的用户的十个相似度
    val df_user_topK = df_sim_rel.selectExpr("user_id", "user_v", "cos", "row_number() over(partition by user_id order by cos desc) as rank ")
      .where("rank<30")
    // 获取用户的课程集合
    val user_item_list = df.rdd.map(row => {
      (row(0).toString, (row(1).toString + "_" + row(2).toString))
    }).groupByKey().mapValues(x => x.toArray).toDF("user_id", "item_list")
    val user_item_list_v = user_item_list
      .selectExpr("user_id as user_v", "item_list as item_list_v")

    //获取相似用户的课程集合函数
    session.udf.register("removeSimi", (item1: Seq[String], item2: Seq[String]) => {
      val map = item1.map(x => {
        val l = x.split("_")
        (l(0), l(1).toDouble)
      }).toMap
      val retn = item2.filter(line => {
        val l = line.split("_")
        map.getOrElse(l(0), -1) != -1
      })
      retn
    })

    // 获取相似用户的课程集合
    val user_dis_item = df_user_topK.join(user_item_list, "user_id")
      .join(user_item_list_v, "user_v")
      .selectExpr("user_id", "user_v", "cast(`cos` as double) as `cos`", "removeSimi(item_list,item_list_v) as items")

    val frame = user_dis_item.selectExpr("user_id", "cos", "explode(items) rec")
      .selectExpr("user_id", "cast(split(rec,'_')[0] as int) item_id", "cos", "cast(split(rec,'_')[1] as DOUBLE) prediction")
      .join(df, Seq("user_id", "item_id"))
      .selectExpr("user_id", "item_id", "rating", "prediction*cos prediction")
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
    session.close()
  }
}