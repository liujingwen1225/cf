package org.recommend.analysis2

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.recommend.util.MysqlUtil

object TotalAnalysis {
  def totalAnalysis(course: DataFrame, studentCoursedDF: DataFrame, sparkSession: SparkSession): Unit = {
    //    今年选课用户
    val students = studentCoursedDF.dropDuplicates("student_id").count()
    //    课程数量
    val names = course.dropDuplicates("name").count()
    //    授课教师数量
    val instructors = course.dropDuplicates("instructor").count()
    //    授课学校数量
    val schools = course.dropDuplicates("school").count()
    // 构建df
    val total1 = sparkSession.createDataFrame(Seq((students, names, instructors, schools)))
      .toDF("student_cnt", "name_cnt", "instructor_cnt", "school_cnt")
    MysqlUtil.writeMysqlTable(SaveMode.Overwrite, total1, "total_analysis")

    //    授课学校数量
    val nameLabels = course.groupBy("labels").count().withColumnRenamed("count","cnt")
    //    授课学校数量
    val nameStatus = course.groupBy("status").count()
      .withColumnRenamed("status","labels")
      .withColumnRenamed("count","cnt")
    val total2 = nameLabels.unionAll(nameStatus)
    MysqlUtil.writeMysqlTable(SaveMode.Overwrite, total2, "total_analysis2")

  }
}
