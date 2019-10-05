package com.mycompany.bigdata.domain

import org.apache.spark.sql.types._

object TelecomData extends Enumeration {

  private val DELIMITER = "\\t"

  val SQUARE_ID, TIME_INTERVAL, COUNTRY_CODE, SMS_IN_ACTIVITY,
      SMS_OUT_ACTIVITY, CALL_IN_ACTIVITY, CALL_OUT_ACTIVITY,
      INTERNET_TRAFIC_ACTIVITY = Value

  val structType = StructType(
    Seq(
      StructField(SQUARE_ID.toString, IntegerType),
      StructField(TIME_INTERVAL.toString, LongType),
      StructField(COUNTRY_CODE.toString, IntegerType),
      StructField(SMS_IN_ACTIVITY.toString, DoubleType),
      StructField(SMS_OUT_ACTIVITY.toString, DoubleType),
      StructField(CALL_IN_ACTIVITY.toString, DoubleType),
      StructField(CALL_OUT_ACTIVITY.toString, DoubleType),
      StructField(INTERNET_TRAFIC_ACTIVITY.toString, DoubleType)
    )
  )

  def apply(row: String): TelecomData = {
    val arrayRows = row.split(DELIMITER, -1)
      TelecomData(
        if(!arrayRows(SQUARE_ID.id).isEmpty){arrayRows(SQUARE_ID.id).toInt}else{0},
        if(!arrayRows(TIME_INTERVAL.id).isEmpty){arrayRows(TIME_INTERVAL.id).toLong}else{0},
        if(!arrayRows(COUNTRY_CODE.id).isEmpty){arrayRows(COUNTRY_CODE.id).toInt}else{0},
        if(!arrayRows(SMS_IN_ACTIVITY.id).isEmpty){arrayRows(SMS_IN_ACTIVITY.id).toDouble}else{0.0},
        if(!arrayRows(SMS_OUT_ACTIVITY.id).isEmpty){arrayRows(SMS_OUT_ACTIVITY.id).toDouble}else{0.0},
        if(!arrayRows(CALL_IN_ACTIVITY.id).isEmpty){arrayRows(CALL_IN_ACTIVITY.id).toDouble}else{0.0},
        if(!arrayRows(CALL_OUT_ACTIVITY.id).isEmpty){arrayRows(CALL_OUT_ACTIVITY.id).toDouble}else{0.0},
        if(!arrayRows(INTERNET_TRAFIC_ACTIVITY.id).isEmpty){arrayRows(INTERNET_TRAFIC_ACTIVITY.id).toDouble}else{0.0}
      )
    }

//  def apply(row: String): TelecomData = {
//    val arrayRows = row.split(DELIMITER, -1)
//    TelecomData(
//      arrayRows(SQUARE_ID.id).toInt,
//      arrayRows(TIME_INTERVAL.id).toLong,
//      arrayRows(COUNTRY_CODE.id).toInt,
//      arrayRows(SMS_IN_ACTIVITY.id).toDouble,
//      arrayRows(SMS_OUT_ACTIVITY.id).toDouble,
//      arrayRows(CALL_IN_ACTIVITY.id).toDouble,
//      arrayRows(CALL_OUT_ACTIVITY.id).toDouble,
//      arrayRows(INTERNET_TRAFIC_ACTIVITY.id).toDouble
//    )
//  }


  case class TelecomData(
                  squareId: Int,
                  timeInterval: Long,
                  countryCode: Int,
                  smsInActivity: Double,
                  smsOutActivity: Double,
                  callInActivity: Double,
                  callOutActivity: Double,
                  internetTraficActivity: Double
                )
}
