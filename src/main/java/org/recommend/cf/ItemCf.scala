package org.recommend.cf

import org.apache.spark.sql.SaveMode
import org.recommend.util.{MysqlUtil, SessionUtil}


/**
 * 基于物品的协同过滤算法
 * item cf
 */
object ItemCf {

  def main(args: Array[String]): Unit = {
    val session = SessionUtil.createSparkSession(this.getClass)

    import org.apache.spark.sql.functions._
    import session.implicits._

    //注册 mysql 用户评分表
    MysqlUtil.readMysqlTable(session, "student_course")
    MysqlUtil.readMysqlTable(session, "course")
    MysqlUtil.readMysqlTable(session, "sys_user")
    val GetUserRatingSql =
      s"""
         |select student_id as user_id,
         |       course_id as item_id,
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
    val df = session.sql(GetUserRatingSql)
    // 这个是在保证每个用户对一个课程只评价一次（即以选了课记一分）
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
    // 获得前12个课程
    val df_topK = df_sim_rel
      .selectExpr("item_id", "item_v", "cos", "row_number() over(partition by item_id order by cos desc) as rank")
      .where("rank <=12")
    // 获取相似的前十个物品的集合
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

    session.udf.register("ff", (item: Seq[String], item2: Seq[String]) => {
      val map = item.map(line => {
        val l = line.split("_")
        (l(0), l(1).toDouble)
      }).toMap

      val retn = item2.filter(line => {
        val l = line.split("_")
        map.getOrElse(l(0), -1) == -1
      })
      retn
    })
    // 过滤掉重复的课程
    val df_rec_tmp = df_u_item.join(df_list, "item_id")
      .selectExpr("user_id", "rating", "ff(item_l,item_list) as rec_list")
      .selectExpr("user_id", "rating", "explode(rec_list) rec")
      .selectExpr("user_id", "split(rec,'_')[0] as item_id", "cast(split(rec,'_')[1] as double)*rating as rating")

    // 获得最终的推荐
    val df_rec = df_rec_tmp.groupBy("user_id", "item_id")
      .agg(sum("rating").as("rating"))
      .selectExpr("user_id", "cast(item_id as integer) as course_id", "row_number() over(partition by user_id order by rating desc) as ranking")
      .where("ranking<=12")
    MysqlUtil.writeMysqlTable(SaveMode.Overwrite,df_rec,"course_recommend_item_cf")

  }
}
