package com.xiajun.bap.release.util

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalUnit


/**
  * 时间处理工具类
  */
object DateUtil {
    /**
      * 给定日期格式化输出
      * @param date
      * @param pattern
      * @return
      */
    def dateFormat4String(date: String, pattern: String = "yyyyMMdd"): String = {
        if (date == null) {
            return null
        }
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
        val localdate = LocalDate.parse(date, formatter)
        localdate.format(formatter)
    }

    /**
      * 给定日期，推迟n天后的结果格式化输出
      * @param date
      * @param diff
      * @param pattern
      * @return
      */
    def dateFormat4StringDiff(date:String,diff:Long,pattern:String="yyyyMMdd"): String ={
        if (date==null) return null
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
        val localDate: LocalDate = LocalDate.parse(date)
        val plused: LocalDate = localDate.plusDays(diff)
        plused.format(formatter)
    }

    /**
      * 给定日期和另外一个日期，给定格式，比较日期大小
      * @param date
      * @param anotherDate
      * @param pattern
      * @return
      */
    def dateCompare(date:String,anotherDate:String,pattern:String="yyyyMMdd"):Boolean={
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
        val date1: LocalDate = LocalDate.parse(date)
        val date2: LocalDate = LocalDate.parse(anotherDate)
        date1.compareTo(date2) > 0
    }

}
