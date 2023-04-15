package org.recommend.analysis

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.recommend.util.MysqlUtil

object TotalAnalysis {
  def totalAnalysis(sparkSession: SparkSession): Unit = {
    //    今年选课用户
    //    课程数量
    //    授课教师数量
    //    授课学校数量
    val sql1 =
    """
      |select *
      |from (select count(*) school_cnt
      |      from (select 1 from course group by school) t) t1
      |         join (select count(*) name_cnt
      |               from (select 1 from course group by name) t) t2
      |         join (select count(*) instructor_cnt
      |               from (select 1 from course group by instructor) t) t3
      |         join (select count(*) student_cnt
      |               from (select 1 from student_course group by student_id) t) t4
      |""".stripMargin
    val frame = sparkSession.sql(sql1)
    val sql2 =
      """
        |select *
        |from (select count(*) school_cnt
        |      from (select 1 from course group by school) t) t1
        |         join (select count(*) name_cnt
        |               from (select 1 from course group by name) t) t2
        |         join (select count(*) instructor_cnt
        |               from (select 1 from course group by instructor) t) t3
        |         join (select count(*) student_cnt
        |               from (select 1 from student_course group by student_id) t) t4
        |""".stripMargin
    val frame2 = sparkSession.sql(sql2)
    MysqlUtil.writeMysqlTable(SaveMode.Overwrite, frame, "total_analysis")
    MysqlUtil.writeMysqlTable(SaveMode.Overwrite, frame2, "total_analysis2")
  }
}
