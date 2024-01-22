package org.person.ip.count.parallelizable

import org.apache.spark.sql.SparkSession

import java.io.File

object CountFilesWIthParallelizable {

  def countAllFilesPar(allFiles: List[String], spark: SparkSession): List[(String, Long)] = {
    allFiles.par.map {
      fileName => {
        val count = spark.read.csv(fileName).count()
        (fileName, count)
      }
    }.toList
  }

}
