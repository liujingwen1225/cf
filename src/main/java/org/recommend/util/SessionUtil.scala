package org.recommend.util

import org.apache.spark.sql.SparkSession


/**
 * sparkSession工具类
 */
object SessionUtil {
  def createSparkSession(clazz: Class[_], partitions: Int = 2): SparkSession = {
    System.setProperty("HADOOP_USER_NAME", "hive")
    SparkSession.builder()
      .appName(clazz.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.sql.partitions", partitions)
      .config("spark.default.parallelism", "2")
      .getOrCreate()
  }
}
