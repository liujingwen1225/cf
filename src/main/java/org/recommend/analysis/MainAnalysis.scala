package org.recommend.analysis

import org.recommend.util.{MysqlUtil, SessionUtil}

object MainAnalysis {
  def main(args: Array[String]): Unit = {
    val session = SessionUtil.createSparkSession(this.getClass)
    MysqlUtil.readMysqlTable(session, "course")

    HotAnalysis.hotAnalysis(session)
    CntAnalysis.cntAnalysis(session)
    TotalAnalysis.totalAnalysis(session)
  }
}
