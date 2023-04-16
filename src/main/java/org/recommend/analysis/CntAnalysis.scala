package org.recommend.analysis

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.recommend.util.MysqlUtil

object CntAnalysis {
  def cntAnalysis(sparkSession: SparkSession): Unit = {
    val sql1 =
      """
        |select type,
        |       school,
        |       count(*) school_cnt
        |from course
        |group by type, school
        |order by school_cnt desc
        |""".stripMargin
    val frame1 = sparkSession.sql(sql1)
    MysqlUtil.writeMysqlTable(SaveMode.Overwrite, frame1, "school_cnt_analysis")
    val sql2 =
      """
        |select type,
        |      count(*) type_cnt
        |      from course
        |      group by type
        |      order by type_cnt desc
        |""".stripMargin
    val frame = sparkSession.sql(sql2)
    MysqlUtil.writeMysqlTable(SaveMode.Overwrite, frame, "type_cnt_analysis")
  }

}
