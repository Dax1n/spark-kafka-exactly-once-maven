package com.daxin

import org.apache.spark.{SparkContext, SparkConf}
import java.sql.{DriverManager, PreparedStatement, Connection}
/**
  * @author ${user.name}
  */
object Main {

  case class record(url: String, nums: Int)

  def toMySQL(iterator: Iterator[(String, Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into record(url, nums) values (?, ?)"
    try {
      Class.forName("com.mysql.jdbc.Driver");
      conn = DriverManager.getConnection("jdbc:mysql://node:3306/log", "root", "root")
      iterator.foreach(dataIn => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, dataIn._1)
        ps.setInt(2, dataIn._2)
        ps.executeUpdate()
      }
      )
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Log-Proccess")//setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    // System.setProperty("hadoop.home.dir", "C:\\hadoop-2.7.2");
    val rdd = sc.textFile("/eshop-log")

    val arrs = rdd.map {
      x =>
        val fs = x.split(" ")(6).split("\\?")(0)
        fs
    }

    val result = arrs.map((_,1)).reduceByKey(_+_)

    result.foreachPartition(toMySQL)

    sc.stop()

  }

}
