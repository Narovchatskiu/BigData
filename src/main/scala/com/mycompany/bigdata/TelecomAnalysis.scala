package com.mycompany.bigdata

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAdjusters
import java.time._
import java.util.{Date, Locale, TimeZone}
import java.time.DayOfWeek

import com.mycompany.Parameters
import com.mycompany.bigdata.domain.TelecomData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
//import org.apache.spark.sql._
//import org.tribbloid.ispark.display.dsl._
//import org.apache.spark.rdd.RDD
import scala.util.Try

object TelecomAnalysis extends App {

  //  Logger.getLogger("org").setLevel(Level.ERROR)
  //  Logger.getLogger("akka").setLevel(Level.ERROR)
  //  Logger.getLogger("netty").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Bigdata-project-for-pollution-analysis")

  val sc = new SparkContext(sparkConf)

  val conf = new Configuration()
  val fs = FileSystem.get(conf)

  fs.delete(new Path(Parameters.OUTPUT_PATH), true)

  val telecom = sc.textFile(Parameters.TELECOM_PATH)
    .map(line => TelecomData(line))
    .map(conv => {
            (conv.squareId,filterWeekend(transferTime(dateTimeConvertor(conv.timeInterval))))
      })
    .cache()
    .coalesce(1)
    .saveAsTextFile(Parameters.OUTPUT_PATH)


  def dateTimeConvertor(number: Long): String = {

    val sdf = new SimpleDateFormat("EEE, MMM d, yyyy-MM-dd hh:mm:ss a z", Locale.ENGLISH)
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    sdf.format(number)

  }

  def transferTime(string: String): LocalDateTime = {

    val splittedValue = string.toString.split(Parameters.INPUT_DELIMITER)
    val dateSplit = splittedValue(2).split("-")

    val dateOktober = YearMonth.of(dateSplit(0).toInt, Month.OCTOBER)
      .atEndOfMonth()
      .`with`(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY))

    val dateMarch = YearMonth.of(dateSplit(0).toInt, Month.MARCH)
      .atEndOfMonth
      .`with`(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY))

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss a z", Locale.ENGLISH)
    val dateLocal = LocalDate.parse(splittedValue(2), formatter)
    val dateLocalTime = LocalDateTime.parse(splittedValue(2), formatter)

    if (dateLocal.isAfter(dateMarch) && dateLocal.isBefore(dateOktober)) {
      dateLocalTime.plusHours(3)
    } else if (dateLocal.isAfter(dateOktober) && dateLocal.isBefore(dateMarch.plusYears(1))) {
      dateLocalTime.plusHours(2)
    } else {
      dateLocalTime
    }

  }

  def filterWeekend(date: LocalDateTime): String ={
    val day = date.getDayOfWeek
    if (!(day eq DayOfWeek.SATURDAY) && !(day eq DayOfWeek.SUNDAY)){
      return date.toString
    }
    "weekend"
  }


}




//  Source.fromFile("./user/tcld/source/TELECOMMUNUCATIONS_MI/november/sms-call-internet-mi-2013-11-01.txt").foreach {
//    print
//  }

//  val data = telecom.flatMap{
//    line => Try
//    {
//        line.split("\t") match{
//        case Array(squareId, timeInterval, countryCode, smsInActivity, smsOutActivity, callInActivity, callOutActivity, internetTraficActivity)
//          =>
//          Row(squareId.toInt, timeInterval.toLong, countryCode.toInt, smsInActivity.toDouble, smsOutActivity.toDouble, callInActivity.toDouble, callOutActivity.toDouble, internetTraficActivity.toDouble)
//      }
//    }.toOption
//  }
//

//  val telecom = sc.textFile("./user/tcld/source/TELECOMMUNUCATIONS_MI/november/sms-call-internet-mi-2013-11-01.txt")
//    .map(textLine => {
//    val dataset = TelecomData(textLine)
//      (dataset.squareId,(dataset.timeInterval,dataset.countryCode, dataset.smsInActivity, dataset.smsOutActivity,
//        dataset.callInActivity, dataset.callOutActivity, dataset.internetTraficActivity))
//
//    }).saveAsTextFile("./src/test/resources/output/")
