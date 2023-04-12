package org.recommend.load

import org.apache.spark.sql.SaveMode
import org.recommend.util.{MysqlUtil, SessionUtil}

object LoadData {
  def main(args: Array[String]): Unit = {
    //课程类型,课程链接,参加人数,授课老师,课程名称,课程学校,课程标签,开课开始时间,开课结束时间,课程概述,课程状态,课程评分,封面图

    val session = SessionUtil.createSparkSession(LoadData.getClass)
      MysqlUtil.readMysqlTable(session,"course_infos")

    val sql =
      """
        |select  row_number() over(ORDER BY `participants_number`) as id,
        |       `type`     as type,
        |       `link`     as link,
        |       `participants_number`     as participants_number,
        |       `instructor`     as instructor,
        |       `name`     as name,
        |       `school`     as school,
        |       `labels`     as labels,
        |       `start_time` as start_time,
        |       `end_time` as end_time,
        |       `overview`     as overview,
        |       `status`     as status,
        |       `grading`     as grading,
        |       `cover_image_url`       as cover_image_url
        |from course_infos where `name` is not null
        |""".stripMargin
    val frame = session.sql(sql)
    frame.show(10)
    MysqlUtil.writeMysqlTable(SaveMode.Overwrite,frame,"course")

  }
}
