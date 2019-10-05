package com.mycompany.bigdata

import java.sql.Date

import org.apache.spark.sql.types.{DateType, IntegerType, StructField, StructType}

object PollutionData extends Enumeration {

  private val DELIMITER = "\\t"

  val SENSOR_ID, TIME_INSTANT, MEASUREMENT = Value

  val structType = StructType(
    Seq(
      StructField(SENSOR_ID.toString, IntegerType),
      StructField(TIME_INSTANT.toString, DateType),
      StructField(MEASUREMENT.toString, IntegerType)
    )
  )

  def apply(row: String): PollutionData = {
    val array = row.split(DELIMITER, -1)
    PollutionData(
      array(SENSOR_ID.id).toInt,
      Date.valueOf(array(TIME_INSTANT.id)),
      array(MEASUREMENT.id).toInt
    )
  }

  case class PollutionData(
                          SenorId: Int,
                          TimeInstant: Date,
                          Measurement: Int,
                          )

}
