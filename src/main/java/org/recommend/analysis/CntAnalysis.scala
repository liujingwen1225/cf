package org.recommend.analysis

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.recommend.util.MysqlUtil

object CntAnalysis {
  def cntAnalysis(sparkSession: SparkSession): Unit = {
    val sql1 =
      """
        |select *,
        |       row_number() over (order by school_cnt desc) rn
        |from (select school,
        |             count(*) school_cnt
        |      from course
        |      group by school
        |      order by school_cnt desc
        |      limit 10) tem
        |""".stripMargin
    sparkSession.sql(sql1).createOrReplaceTempView("t1")

    val sql2 =
      """
        |select *,
        |       row_number() over (order by type_cnt desc) rn
        |from (select type,
        |             count(*) type_cnt
        |      from course
        |      group by type
        |      order by type_cnt desc
        |      limit 10) tem
        |""".stripMargin
    sparkSession.sql(sql2).createOrReplaceTempView("t2")
    val sql3 =
      """
        |select t1.rn, type, type_cnt, school, school_cnt
        |from t1
        |          join t2 on t1.rn = t2.rn;
        |""".stripMargin
    val frame = sparkSession.sql(sql3)
    MysqlUtil.writeMysqlTable(SaveMode.Overwrite, frame, "cnt_analysis")
  }

}
