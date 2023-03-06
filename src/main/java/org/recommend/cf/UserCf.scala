package org.recommend.cf

import org.apache.spark.sql.SaveMode
import org.recommend.util.{MysqlUtil, SessionUtil}

/**
 * 基于用户的协同过滤算法
 * usercf
 */
object UserCf {
  def main(args: Array[String]): Unit = {
    val session = SessionUtil.createSparkSession(this.getClass)
    import org.apache.spark.sql.functions._
    import session.implicits._

    //注册 mysql 用户评分表
    MysqlUtil.readMysqlTable(session, "student_course")
    MysqlUtil.readMysqlTable(session, "course")
    // 使用余弦相似度计算用户相似度
    val GetUserRatingSql =
    s"""
       |select st.student_id as user_id, st.course_id as item_id, (ifnull(st.rating, 2) * 0.4 + c.grading * 0.6) rating
       |from student_course st
       |         left join course c on c.id = st.course_id
       |where grading is not null
      """.stripMargin
    println("加载评分表")
    val df = session.sql(GetUserRatingSql)
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
    val k = 12
    val df_user_topK = df_sim_rel.selectExpr("user_id", "user_v", "cos", "row_number() over(partition by user_id order by cos desc) as rank ")
      .where("rank<=" + k)
    println("用户相似")
    df_user_topK.show(100)

    // 获取用户的物品集合（未去除用户已经选了的课程）
    val user_item_list = df.rdd.map(row => {
      (row(0).toString, (row(1).toString + "_" + row(2).toString))
    }).groupByKey().mapValues(x => x.toArray).toDF("user_id", "item_list")

    // 对用户获得的物品集合进行去重
    val user_item_list_v = user_item_list
      .selectExpr("user_id as user_v", "item_list as item_list_v")

    //去重函数
    session.udf.register("removeSimi", (item1: Seq[String], item2: Seq[String]) => {
      val map = item1.map(x => {
        val l = x.split("_")
        (l(0), l(1).toDouble)
      }).toMap
      val retn = item2.filter(line => {
        val l = line.split("_")
        map.getOrElse(l(0), -1) == -1
      })
      retn
    })

    // 去除已经选了的课程，得出要推荐的课程
    val user_dis_item = df_user_topK.join(user_item_list, "user_id")
      .join(user_item_list_v, "user_v")
      .selectExpr("user_id", "cast(`cos` as double) as `cos`", "removeSimi(item_list,item_list_v) as items")

    // 计算物品的评分函数
    session.udf.register("calItemRating", (cos: Double, items: Seq[String]) => {
      val tu = items.map(line => {
        val l = line.split("_")
        (l(0), l(1).toDouble * cos)
      })
      tu
    })

    // 对用户进行推荐的课程评分
    val user_rec_tmp = user_dis_item.selectExpr("user_id", "calItemRating(cos,items) as items").selectExpr("user_id", "explode(items) as tu").selectExpr("user_id", "tu._1 as item_id", "tu._2 as rating")
      .groupBy("user_id", "item_id")
      .agg(sum("rating").as("rating"))
    // 排序得出前12个推荐课程
    val user_rec = user_rec_tmp.selectExpr("user_id", "item_id as course_id", "row_number() over(partition by user_id order by rating desc) as ranking")
      .where("ranking<=" + k)
    println("推荐结果")
    MysqlUtil.writeMysqlTable(SaveMode.Overwrite,user_rec,"course_recommend")

  }
}