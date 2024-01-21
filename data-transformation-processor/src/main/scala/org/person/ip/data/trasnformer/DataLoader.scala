package org.person.ip.data.trasnformer

import org.apache.spark.sql.{SaveMode, SparkSession}

object DataLoader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("PersonIpDataLoader")
      .master("local[*]")
      .getOrCreate()


    val personIpDf = spark.read.option("header",value = true)
      .csv("/Users/adnanrahin/Desktop/pii_data_compressed/*")


    println(personIpDf.count())

  }

}
