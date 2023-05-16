package org.recommend.analysis2


import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.types.LongType
import org.recommend.util.MysqlUtil

object HotAnalysis {

  def hotAnalysis(course: DataFrame): Unit = {

    // 各类型下排名前十的课程
    val name_num1 = course.withColumn("participants_number", col("participants_number").cast(LongType))
    val name_num2 = name_num1.groupBy("id", "type", "name")
      .sum("participants_number")
      .withColumnRenamed("sum(participants_number)", "name_num")
    val windowSpec1 = Window.partitionBy("type").orderBy(col("name_num").desc)
    val name_num3 = name_num2.withColumn("rn", row_number().over(windowSpec1))
      .where("rn<=10")


    // 各类型下排名前十的学校
    val school_num1 = course.withColumn("participants_number", col("participants_number").cast(LongType))
    val school_num2 = school_num1.groupBy("id", "type", "school")
      .sum("participants_number")
      .withColumnRenamed("sum(participants_number)", "school_num")
    val windowSpec2 = Window.partitionBy("type").orderBy(col("school_num").desc)
    val school_num3 = school_num2.withColumn("rn", row_number().over(windowSpec2))
      .where("rn<=10")


    // 各类型下排名前十的老师
    val instructor_num1 = course.withColumn("participants_number", col("participants_number").cast(LongType))
    val instructor_num2 = instructor_num1.groupBy("id", "type", "instructor")
      .sum("participants_number")
      .withColumnRenamed("sum(participants_number)", "instructor_num")
    val windowSpec3 = Window.partitionBy("type").orderBy(col("instructor_num").desc)
    val instructor_num3 = instructor_num2.withColumn("rn", row_number().over(windowSpec3))
      .where("rn<=10")

    // 关联结果集
    val final_result =
    name_num3
      .join(school_num3, Seq("rn", "type"))
      .join(instructor_num3, Seq("rn", "type"))
      .select("type", "name", "name_num", "school", "school_num", "instructor", "instructor_num", "rn")

    MysqlUtil.writeMysqlTable(SaveMode.Overwrite, final_result, "hot_analysis")
  }

}
