package org.recommend.load

import org.apache.spark.sql.SaveMode
import org.recommend.util.{MysqlUtil, SessionUtil}

import scala.collection.mutable


/**
 * 模拟用户行为数据
 */
object MockData {

  case class StuCourse(student_id: Int, course_id: Int)

  def main(args: Array[String]): Unit = {
    //    Test
    val session = SessionUtil.createSparkSession(LoadData.getClass)

    val StuCourseList = mutable.MutableList[StuCourse]()

    for (i <- 33 to 132) {
      // 5-20
      val randomCourseNum = scala.util.Random.nextInt(20 - 5 + 1) + 5
      val ints = RandomCourseList(randomCourseNum)
      for (elem <- ints) {
        StuCourseList += StuCourse(i, elem)
      }
    }
    //    StuCourseList.foreach(i => println(i))
    val StuCourseDF = session.createDataFrame(StuCourseList)
    StuCourseDF.show(100)
    MysqlUtil.writeMysqlTable(SaveMode.Append,StuCourseDF, "student_course")

  }

  def RandomCourseList(n: Int) = {
    var outList: Set[Int] = Set()
    do {
      outList += scala.util.Random.nextInt(609 - 308 + 1) + 308
    } while (outList.size < n)
    outList
  }
}
