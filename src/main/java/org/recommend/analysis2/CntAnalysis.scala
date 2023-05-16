package org.recommend.analysis2

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.recommend.util.MysqlUtil

object CntAnalysis {
  def cntAnalysis(course: DataFrame): Unit = {
    // 各类型下的学校的课程数
    val frame1 = course.groupBy("type", "school")
      .count().withColumnRenamed("count", "school_cnt")

    // 各类型的课程数
    val frame2 = course.groupBy("type")
      .count().withColumnRenamed("count", "type_cnt")

    MysqlUtil.writeMysqlTable(SaveMode.Overwrite, frame1, "school_cnt_analysis")
    MysqlUtil.writeMysqlTable(SaveMode.Overwrite, frame2, "type_cnt_analysis")
  }

}
