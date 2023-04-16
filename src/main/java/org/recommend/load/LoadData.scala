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
        |select row_number() over (ORDER BY `participants_number`) as id,
        |       type,
        |       link,
        |       participants_number,
        |       instructor,
        |       name,
        |       school,
        |       labels,
        |       start_time,
        |       start_time_date,
        |       end_time,
        |       end_time_date,
        |       overview,
        |       case when (start_time_date < now() and end_time_date > now()) then '课程进行中'  WHEN end_time_date < now() then '课程已结束' when start_time_date > now() then '课程即将开始' ELSE '未知' END AS status,
        |       grading,
        |       cover_image_url
        |from (select row_number() over (partition by name,school ORDER BY `participants_number`) as rn,
        |             `type`                                                                 as type,
        |             `link`                                                                 as link,
        |              ifnull(`participants_number`, 0)                                      as participants_number,
        |             `instructor`                                                           as instructor,
        |             `name`                                                                 as name,
        |             `school`                                                               as school,
        |             `labels`                                                               as labels,
        |             `start_time`                                                           as start_time,
        |             cast(regexp_replace(regexp_replace(regexp_replace(`start_time`,'年','-'),'月','-'),'日','') as date)   as start_time_date,
        |             `end_time`                                                             as end_time,
        |             cast(regexp_replace(regexp_replace(regexp_replace(`end_time`,'年','-'),'月','-'),'日','') as date)  as end_time_date,
        |             `overview`                                                             as overview,
        |             `status`                                                               as status,
        |             ifnull(`grading`, 0)                                                   as grading,
        |             `cover_image_url`                                                      as cover_image_url
        |      from course_infos
        |      where `name` is not null
        |        and `instructor` is not null
        |        and `start_time` < '2024-01-01'
        |        and `end_time` < '2024-01-01') t
        |where rn = 1
        |""".stripMargin
    val frame = session.sql(sql)
//    frame.show(10)
    MysqlUtil.writeMysqlTable(SaveMode.Overwrite,frame,"course")

  }
}
