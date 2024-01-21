package org.person.ip.count.recurssion

import org.apache.spark.sql.SparkSession

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util.concurrent.Executors
import java.util.zip.GZIPInputStream
import scala.collection.parallel.ExecutionContextTaskSupport
import scala.concurrent.ExecutionContext

object CountFileLinesRecursion {

  private def countAllFilesFuture(allFiles: List[String], spark: SparkSession): List[(String, Long)] = {

    val availableProcessor = Runtime.getRuntime.availableProcessors()
    val threadPool = Executors.newFixedThreadPool(availableProcessor)
    val executionContext = ExecutionContext.fromExecutor(threadPool)

    val fileNameExtensionFilter = allFiles.par
    fileNameExtensionFilter.tasksupport = new ExecutionContextTaskSupport(executionContext)

    val recordsFileCounts: List[(String, Long)] =
      fileNameExtensionFilter.map{ fileName =>
        val count = countLinesRecursive(fileName, 4096 * 4096)
        (fileName, count)
      }.toList

    threadPool.shutdown()

    recordsFileCounts

  }


  private def countLines(file: String): Long = {
    try {
      val input = new GZIPInputStream(new FileInputStream(file))
      val reader = new BufferedReader(new InputStreamReader(input))
      val linesInCurrentChunk = Iterator.continually(reader.readLine()).takeWhile(_ != null).length.toLong
      reader.close()
      input.close()
      linesInCurrentChunk
    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        0L
    }
  }

  private def countLinesRecursive(file: String, chunkSize: Int): Long = {
    try {
      val fileLength = new java.io.File(file).length()

      if (fileLength <= chunkSize) {
        countLines(file)
      } else {
        val halfSize = fileLength / 2

        val leftHalf = countLinesRecursive(file, chunkSize, 0, halfSize)
        val rightHalf = countLinesRecursive(file, chunkSize, halfSize, fileLength)

        leftHalf + rightHalf
      }
    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        0L
    }
  }

  private def countLinesRecursive(file: String, chunkSize: Int, start: Long, end: Long): Long = {
    try {
      val input = new GZIPInputStream(new FileInputStream(file))
      val reader = new BufferedReader(new InputStreamReader(input))

      reader.skip(start)

      val linesInChunk = Iterator.continually(reader.readLine()).takeWhile(_ != null).length.toLong

      reader.close()
      input.close()

      linesInChunk
    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        0L
    }
  }


}
