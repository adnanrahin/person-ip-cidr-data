package org.person.ip.count.recurssion

import java.io.FileInputStream
import java.util.zip.GZIPInputStream
import scala.concurrent.Future
import scala.io.Source
import org.apache.spark.sql.SparkSession
import java.util.concurrent.Executors
import scala.collection.parallel.ExecutionContextTaskSupport
import scala.concurrent.ExecutionContext
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object CountFileLinesUsingRecursion {

  private def multiThreadedCountJob(allFiles: List[String], spark: SparkSession): List[(String, Long)] = {

    val availableProcessor = Runtime.getRuntime.availableProcessors()
    val threadPool = Executors.newFixedThreadPool(availableProcessor)
    val executionContext = ExecutionContext.fromExecutor(threadPool)

    val fileNameExtensionFilter = allFiles.par
    fileNameExtensionFilter.tasksupport = new ExecutionContextTaskSupport(executionContext)

    val recordsFileCounts: List[(String, Long)] =
      fileNameExtensionFilter.map{ fileName =>
        val countFuture: Future[Long] = countLinesConcurrently(fileName)
        val count: Long = Await.result(countFuture, Duration.Inf) // Block for the result
        (fileName, count)
      }.toList

    threadPool.shutdown()

    recordsFileCounts

  }

  private def countLinesInChunk(filename: String, chunkSize: Int): Int = {
    val stream = new GZIPInputStream(new FileInputStream(filename))
    stream.skip(chunkSize)
    val lines = Source.fromInputStream(stream).getLines().size
    stream.close()
    lines
  }

  private def countLinesConcurrently(filename: String, chunkSize: Long = 4096 * 4096): Future[Long] = {
    val fileSize = new GZIPInputStream(new FileInputStream(filename)).available()
    val chunks = fileSize / chunkSize + (if (fileSize % chunkSize > 0) 1 else 0)

    if (chunks == 1) {
      Future {
        countLinesInChunk(filename, fileSize.toInt)
      }
    } else {
      val (leftChunks, rightChunks) = chunks / 2 -> (chunks - chunks / 2)
      val leftFuture = countLinesConcurrently(filename, chunkSize * leftChunks)
      val rightFuture = countLinesConcurrently(filename, chunkSize * rightChunks)
      for (leftCount <- leftFuture; rightCount <- rightFuture) yield leftCount + rightCount
    }
  }

}
