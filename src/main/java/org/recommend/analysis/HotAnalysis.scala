package org.recommend.analysis

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.recommend.util.MysqlUtil

object HotAnalysis {

  def hotAnalysis(sparkSession: SparkSession): Unit = {
    val sql1 =
      """
        |select id, type, name, name_num,rn
        |from (select *, row_number() over (partition by type order by name_num desc)  rn
        |      from (select id, type, name, sum(participants_number) as name_num
        |            from course
        |            group by id, type, name
        |            order by type, name_num desc) tem) tem
        |where rn <= 10
        |order by type,name_num
        |""".stripMargin
    sparkSession.sql(sql1).createOrReplaceTempView("t1")

    val sql2 =
      """
        |select id, type, school,school_num,rn
        |from (select *, row_number() over (partition by type order by school_num desc)  rn
        |      from (select id, type, school, sum(participants_number) as school_num
        |            from course
        |            group by id, type, school
        |            order by type, school_num desc) tem) tem
        |where rn <= 10
        |order by type,school_num
        |""".stripMargin
    sparkSession.sql(sql2).createOrReplaceTempView("t2")

    val sql3 =
      """
        |select id, type, instructor, instructor_num,rn
        |from (select *, row_number() over (partition by type order by instructor_num desc)  rn
        |      from (select id, type, instructor, sum(participants_number) as instructor_num
        |            from course
        |            group by id, type, instructor
        |            order by type, instructor_num desc) tem) tem
        |where rn <= 10
        |order by type,instructor_num
        |""".stripMargin
    sparkSession.sql(sql3).createOrReplaceTempView("t3")

    val sql4 =
      """
        |select t1.type, name, name_num, school, school_num, instructor, instructor_num,t1.rn
        |from t1
        |         left join t2 on t1.rn = t2.rn and t1.type = t2.type
        |         left join t3 on t1.rn = t3.rn and t1.type = t3.type;
        |""".stripMargin
    val frame = sparkSession.sql(sql4)
    MysqlUtil.writeMysqlTable(SaveMode.Overwrite, frame, "hot_analysis")
  }

}
