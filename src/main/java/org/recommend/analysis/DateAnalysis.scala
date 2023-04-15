package org.recommend.analysis

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.recommend.util.MysqlUtil

object DateAnalysis {
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


//    MysqlUtil.writeMysqlTable(SaveMode.Overwrite, frame, "cnt_analysis")
  }

}
