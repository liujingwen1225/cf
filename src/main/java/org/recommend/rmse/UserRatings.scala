package org.recommend.rmse

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.recommend.util.MysqlUtil

object UserRatings {
  def getRatings(session: SparkSession): DataFrame = {
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
    session.sql(GetUserRatingSql)
  }
}
