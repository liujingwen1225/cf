package org.recommend.load

import org.apache.spark.sql.SaveMode
import org.recommend.util.{MysqlUtil, SessionUtil}

object LoadData {
  def main(args: Array[String]): Unit = {
    //课程类型,课程链接,参加人数,授课老师,课程名称,课程学校,课程标签,开课开始时间,开课结束时间,课程概述,课程状态,课程评分,封面图

    val session = SessionUtil.createSparkSession(LoadData.getClass)
    session
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("src/main/resources/data/CurtainLesson.csv")
      .createOrReplaceTempView("curtain_lesson")
    val sql =
      """
        |select `课程类型`     as type,
        |       `课程链接`     as link,
        |       `参加人数`     as participants_number,
        |       `授课老师`     as instructor,
        |       `课程名称`     as name,
        |       `课程学校`     as school,
        |       `课程标签`     as labels,
        |       `开课开始时间` as start_time,
        |       `开课结束时间` as end_time,
        |       `课程概述`     as overview,
        |       `课程状态`     as status,
        |       `课程评分`     as grading,
        |       `封面图`       as cover_image_url
        |from curtain_lesson where `课程名称` is not null
        |""".stripMargin
    val frame = session.sql(sql)
    frame.show(10)
    MysqlUtil.writeMysqlTable(SaveMode.Overwrite,frame,"course")

  }
}
