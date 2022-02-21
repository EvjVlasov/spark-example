package com.example.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount")

    val lines = sc.textFile("book.txt")

    val words = lines.flatMap(x => x.split("\\W+"))

    val lowerCaseWorlds = words.map(x => x.toLowerCase())

    val wordCounts = lowerCaseWorlds.map(x => (x, 1)).reduceByKey( (x, y) => x + y)

    val wordsCountsSorted = wordCounts.map(x => (x._2, x._1)).sortByKey()

    for (result <- wordsCountsSorted.collect()) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
  }
}
