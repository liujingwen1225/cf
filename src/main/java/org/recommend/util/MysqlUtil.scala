package org.recommend.util

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

/**
 * mysql 读写工具类
 */
object MysqlUtil {
  private val url = "jdbc:mysql://43.136.26.67:13306/course-recommend?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8&rewriteBatchedStatements=true"
  private val prop = new Properties
  prop.setProperty("user", "root")
  prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
  prop.setProperty("password", "password")

  /**
   * @param spark
   * @param tableName
   */
  def readMysqlTable(spark: SparkSession, tableName: String): Unit = {
    spark.read.jdbc(url, tableName, prop).createOrReplaceTempView(tableName)
  }

  /**
   *
   * @param DF
   * @param tableName
   */
  def writeMysqlTable(mode: SaveMode, DF: DataFrame, tableName: String): Unit = {
    DF.write.mode(mode).jdbc(url, tableName, prop)
  }

}
