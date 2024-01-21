package org.person.ip.count

import org.apache.spark.sql.SparkSession
import org.person.ip.count.parallelizable.CountFilesWIthParallelizable

import java.io.{BufferedReader, File, FileInputStream}
import java.util.concurrent.Executors
import scala.collection.parallel.ExecutionContextTaskSupport
import scala.concurrent.ExecutionContext
import java.util.zip.GZIPInputStream
import scala.io.Source
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import java.io.{BufferedReader, FileInputStream, InputStreamReader}


object CountProcessor {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()

    val spark = SparkSession
      .builder()
      .appName("CountFilesLines")
      .master("local[*]")
      .getOrCreate()

    val inputDataPath: String = "/Users/adnanrahin/Desktop/pii_data_compressed/"

    val allFiles: List[String] = getAllFiles(inputDataPath)

    val filesCount: List[(String, Long)] = CountFilesWIthParallelizable.countAllFilesPar(allFiles, spark)

    filesCount.foreach(println)

    val endTime = System.currentTimeMillis()
    val elapsedTimeMillis = endTime - startTime
    val elapsedTimeMinutes = elapsedTimeMillis / 60000.0 // Convert milliseconds to minutes

    println("Elapsed time: " + elapsedTimeMillis + "ms (" + elapsedTimeMinutes + " minutes)")

  }

  private def getAllFiles(path: String): List[String] = {
    val file = new File(path)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.startsWith("part"))
      .map(_.getPath).toList
  }

}
