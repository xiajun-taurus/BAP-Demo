package com.xiajun.bap.release.util

import com.xiajun.bap.release.exception.InvalidRangeException
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Spark工具类
  */
object SparkHelper {
    private val logger: Logger = LoggerFactory.getLogger(SparkHelper.getClass)

    /**
      * 读取数据
      */
    def readTableData(spark: SparkSession, tableName: String, colNames: mutable.Seq[String]): DataFrame = {
        //获取数据
        val tableDF: DataFrame = spark.read.table(tableName)
            .selectExpr(colNames: _*)
        tableDF
    }

    /**
      * 写入数据
      */
    def writeTableData(sourceDF: DataFrame, table: String, mode: SaveMode): Unit = {
        //写入表
        sourceDF.write.mode(mode).insertInto(table)
    }

    /**
      * 创建SparkSession
      */
    def createSparkSession(conf: SparkConf): SparkSession = {
        SparkSession.builder()
            .config(conf)
            .enableHiveSupport()
            .master("local[*]")
            .getOrCreate()
        //TODO 加载自定义函数

    }

    /**
      * 参数校验
      */
    def rangDates(begin: String, end: String): mutable.Seq[String] = {
        val bdp_days = new ArrayBuffer[String]()
        try {
            val bdp_date_begin: String = DateUtil.dateFormat4String(begin,"yyyy-MM-dd")
            val bdp_date_end: String = DateUtil.dateFormat4String(end,"yyyy-MM-dd")
            //如果开始时间大于结束时间，抛出异常
            if (DateUtil.dateCompare(bdp_date_begin, bdp_date_end, "yyyy-MM-dd")) {
                throw new InvalidRangeException("起始日期不能大于结束日期")
            }
            //如果两个日期相等，取开始时间；反之计算时间差
            if (bdp_date_begin.equals(bdp_date_end)) {
                bdp_days += bdp_date_begin
            } else {
                var tmp = bdp_date_begin
                while (!tmp.equals(bdp_date_end)) {
                    bdp_days += tmp
                    tmp = DateUtil.dateFormat4StringDiff(tmp, 1l, "yyyy-MM-dd")
                }
            }
        } catch {
            case e1: InvalidRangeException => logger.error(e1.getMessage, e1)
            case e2: Exception => logger.error(e2.getMessage, e2)
        }
        bdp_days
    }
}
