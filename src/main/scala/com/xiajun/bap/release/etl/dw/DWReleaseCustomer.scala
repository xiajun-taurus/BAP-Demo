package com.xiajun.bap.release.etl.dw

import com.xiajun.bap.release.constant.ReleaseConstant
import com.xiajun.bap.release.enums.ReleaseStatusEnum
import com.xiajun.bap.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * DW 投放目标客户主题
  */
class DWReleaseCustomer {
}

object DWReleaseCustomer {
    // 日志处理
    private val logger: Logger = LoggerFactory.getLogger(DWReleaseCustomer.getClass)

    /**
      * 目标客户
      */
    def handleReleaseJob(spark: SparkSession, appName: String, bdp_day: String) = {
        val begin = System.currentTimeMillis()
        try {
            //导入隐式转换
            import org.apache.spark.sql.functions._

            //设置缓存级别
            val storageLevel: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
            val saveMdoe = SaveMode.Overwrite
            //获取日志字段数据
            val customerColumns: ArrayBuffer[String] = DWRealseColumnsHelper.selectDWReleaseCustomerColumns()
            //设置条件 当天数据 获取目标客户：01
            val cusomerReleaseCondition = (col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day)
                and
                col(s"${ReleaseConstant.COL_RELEASE_SESSION_STATUS}") === lit(ReleaseStatusEnum.CUSTOMER.getCode))

            val customerReleaseDF: DataFrame = SparkHelper.readTableData(spark, ReleaseConstant.ODS_RELEASE_SESSION, customerColumns)
                //填入条件
                .where(cusomerReleaseCondition)
                //重分区
                .repartition(ReleaseConstant.DEF_SOURCE_PARTITION)
            //打印
//            customerReleaseDF.show(10)
            //目标用户存储
            SparkHelper.writeTableData(customerReleaseDF, ReleaseConstant.DW_RELEASE_CUSTOMER, saveMdoe)
        } catch {
            //错误信息处理
            case ex: Exception => {
                logger.error(ex.getMessage, ex)
            }
        } finally {
            //任务处理时长
            val msg = s"任务 ${appName}-bdp_day = ${bdp_day} 处理时长: ${System.currentTimeMillis() - begin}ms"
            logger.info(msg)
        }
    }

    /**
      * 投放目标用户
      */
    def handleJobs(appName: String, bdp_day_start: String, bdp_day_end: String) = {
        var spark: SparkSession = null

        try {
            //配置Spark参数
            val conf = new SparkConf()
                .set("hive.exec.dynamic.partition", "true")
                .set("hive.exec.dynamic.partition.mode", "nonstrict")
                .set("spark.sql.shuffle.partitions", "32")
                .set("hive.merge.mapfiles", "true")
                .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
                .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
                .set("spark.sql.crossJoin.enabled", "true")
            //创建上下文
            val session: SparkSession = SparkHelper.createSparkSession(conf)

            //解析参数
            val dateRange: mutable.Seq[String] = SparkHelper.rangDates(bdp_day_start,bdp_day_end)

            for (elem <- dateRange) {
                val bdp_day: String = elem.toString
                handleReleaseJob(session,appName,bdp_day)
            }

        } catch {
            case ex: Exception => {
                logger.error(ex.getMessage, ex)
            }
        }
    }

    def main(args: Array[String]): Unit = {
        val appName = "dw_release_job"
        val bdp_day_begin = "2019-09-24"
        val bdp_day_end = "2019-09-24"
        // 执行Job
        handleJobs(appName,bdp_day_begin,bdp_day_end)
    }
}
