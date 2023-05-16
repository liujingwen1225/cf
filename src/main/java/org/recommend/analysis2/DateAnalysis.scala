package org.recommend.analysis2

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.recommend.util.MysqlUtil

object DateAnalysis {
  def dateAnalysis(course: DataFrame): Unit = {

    // 年月处理
    val ym = course
      .withColumn("year", year(course.col("start_time_date")))
      .withColumn("ymonth", month(course.col("start_time_date")))
      .withColumn("participants_number", col("participants_number").cast(LongType))

    // 各年月下的热门课程
    val hotCourse = ym
      .groupBy("year", "ymonth","name")
      .sum("participants_number").withColumnRenamed("sum(participants_number)","name_pns")
      .sort(desc("name_pns"))
    val windowSpec = Window.partitionBy("year","ymonth").orderBy(col("name_pns").desc)
    val fhotCourse = hotCourse.withColumn("rn", row_number().over(windowSpec))
      .where("rn<=10")

    // 各年月下的热门老师
    val hotInstructor = ym
      .groupBy("year", "ymonth", "instructor")
      .sum("participants_number").withColumnRenamed("sum(participants_number)", "instructor_pns")
      .sort(desc("instructor_pns"))
    val windowSpec2 = Window.partitionBy("year", "ymonth").orderBy(col("instructor_pns").desc)
    val fhotInstructor = hotInstructor.withColumn("rn", row_number().over(windowSpec2))
      .where("rn<=10")


    // 各年月下的热门学校
    val hotSchool = ym
      .groupBy("year", "ymonth", "school")
      .sum("participants_number").withColumnRenamed("sum(participants_number)", "school_pns")
      .sort(desc("school_pns"))
    val windowSpec3 = Window.partitionBy("year", "ymonth").orderBy(col("school_pns").desc)
    val fhotSchool = hotSchool.withColumn("rn", row_number().over(windowSpec3))
      .where("rn<=10")
    // 关联结果
    val frame = fhotCourse
      .join(fhotInstructor, Seq("rn", "year", "ymonth"))
      .join(fhotSchool, Seq("rn", "year", "ymonth"))
      .select("year", "ymonth", "name", "name_pns", "instructor", "instructor_pns", "school", "school_pns", "rn")

    MysqlUtil.writeMysqlTable(SaveMode.Overwrite, frame, "year_month_analysis")

  }

}
