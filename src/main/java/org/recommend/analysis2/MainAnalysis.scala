package org.recommend.analysis2

import org.recommend.util.{MysqlUtil, SessionUtil}

object MainAnalysis {
  def main(args: Array[String]): Unit = {
    val session = SessionUtil.createSparkSession(this.getClass)
    val courseDF = MysqlUtil.readMysqlTable(session, "course").toDF()
    val studentCoursedDF = MysqlUtil.readMysqlTable(session, "student_course")

    HotAnalysis.hotAnalysis(courseDF)
    CntAnalysis.cntAnalysis(courseDF)
    TotalAnalysis.totalAnalysis(courseDF,studentCoursedDF,session)
    DateAnalysis.dateAnalysis(courseDF)
  }
}
