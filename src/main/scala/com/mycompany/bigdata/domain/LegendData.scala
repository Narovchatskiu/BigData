package com.mycompany.bigdata

import org.apache.spark.sql.types._

object LegendData extends Enumeration {

  private val DELIMITER = "\\t"

  val SENSOR_ID, SENSOR_STREET_NAME, SENSOR_LAT, SENSOR_LONG, SENSOR_TYPE,
  UOM, TIME_INSTANT_FORMAT = Value

  val structType = StructType(
    Seq(
      StructField(SENSOR_ID.toString, IntegerType),
      StructField(SENSOR_STREET_NAME.toString, StringType),
      StructField(SENSOR_LAT.toString, DoubleType),
      StructField(SENSOR_LONG.toString, DoubleType),
      StructField(SENSOR_TYPE.toString, StringType),
      StructField(UOM.toString, StringType),
      StructField(TIME_INSTANT_FORMAT.toString, StringType)
    )
  )

    def apply(row: String): LegendData = {
      val array = row.split(DELIMITER, -1)
      LegendData(
        array(SENSOR_ID.id).toInt,
        array(SENSOR_STREET_NAME.id),
        array(SENSOR_LAT.id).toInt,
        array(SENSOR_LONG.id).toInt,
        array(SENSOR_TYPE.id),
        array(UOM.id),
        array(TIME_INSTANT_FORMAT.id)
      )
    }

  case class LegendData(
                         SensorID: Int,
                         SensorStreetName: String,
                         SensorLat: Double,
                         SensorLong: Double,
                         SensorType: String,
                         UOM: String,
                         TimeInstantFormat: String
                       )

}
