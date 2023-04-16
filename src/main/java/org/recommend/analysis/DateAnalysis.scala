package org.recommend.analysis

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.recommend.util.MysqlUtil

object DateAnalysis {
  def dateAnalysis(sparkSession: SparkSession): Unit = {
    val sql1 =
      """
        |select *
        |from (
        |         select *, row_number() over (partition by `year`,`ymonth` order by name_pns desc) rn
        |         from (select year(start_time_date) as `year`,month(start_time_date) as `ymonth`, name, sum(participants_number) name_pns
        |from course
        |group by year(start_time_date),month(start_time_date), name order by year,ymonth, name_pns desc) tem
        |     ) t where rn =5;
        |""".stripMargin
    sparkSession.sql(sql1).createOrReplaceTempView("t1")

    val sql2 =
      """
        |select *
        |from (
        |         select *, row_number() over (partition by `year`,`ymonth` order by instructor_pns desc) rn
        |         from (select year(start_time_date) as `year`,month(start_time_date) as `ymonth`, instructor, sum(participants_number) instructor_pns
        |from course
        |group by year(start_time_date),month(start_time_date), instructor order by year ,ymonth ,instructor_pns desc) tem
        |     ) t where rn =5;
        |""".stripMargin
    sparkSession.sql(sql2).createOrReplaceTempView("t2")
    val sql3 =
      """
        |select *
        |from (
        |         select *, row_number() over (partition by `year`,`ymonth` order by school_pns desc) rn
        |         from (select year(start_time_date) as `year`,month(start_time_date) as `ymonth`, school, sum(participants_number) school_pns
        |from course
        |group by year(start_time_date),month(start_time_date), school order by year ,ymonth ,school_pns desc) tem
        |     ) t where rn =5;
        |""".stripMargin
     sparkSession.sql(sql3).createOrReplaceTempView("t3")
  val sql  =
    """
      |select t1.year ,t1.ymonth,name,name_pns ,instructor ,instructor_pns ,school ,school_pns from
      |t1 left join  t2 on t1.rn =t2.rn and t1.year =t2.year and t1.ymonth =t2.ymonth
      |left join  t3 on t1.rn =t3.rn and t1.year =t3.year and t1.ymonth =t3.ymonth
      |""".stripMargin
    val frame = sparkSession.sql(sql)
    MysqlUtil.writeMysqlTable(SaveMode.Overwrite, frame, "year_month_analysis")
  }

}
