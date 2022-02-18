package com.example.spark

import org.apache.spark._
import org.apache.log4j._

object FriendsByAge {

  def parseLine(line:String): (String, Int) = {

    val fields = line.split(",")

    val name = fields(1)
    val age = fields(2).toInt

    (name, age)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")

    val lines = sc.textFile("fakefriends.csv")

    val rdd = lines.map(parseLine)

    val rddCount = rdd.mapValues(x => (x, 1))

    val totalsByName = rddCount.reduceByKey((x, y) => (x._1+y._1, x._2+y._2))

    val averageAgeByName = totalsByName.mapValues(x => x._1/x._2)

    val results = averageAgeByName.collect()

    results.sorted.foreach(println)


    sc.stop()

  }

}
