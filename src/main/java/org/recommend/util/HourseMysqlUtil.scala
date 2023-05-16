package org.recommend.util

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

/**
 * mysql 读写工具类
 */
object HourseMysqlUtil {
  private val url = "jdbc:mysql://4x.13x.2x.x7:1330x/house_db?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8&rewriteBatchedStatements=true"
  private val prop = new Properties
  prop.setProperty("user", "root")
  prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
  prop.setProperty("password", "password")

  /**
   * @param spark
   * @param tableName
   */
  def readMysqlTable(spark: SparkSession, tableName: String): DataFrame = {
    val frame = spark.read.jdbc(url, tableName, prop)
    frame.createOrReplaceTempView(tableName)
    frame
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
