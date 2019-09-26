package com.xiajun.bap.release.etl.dw

import com.xiajun.bap.release.constant.ReleaseConstant
import com.xiajun.bap.release.enums.ReleaseStatusEnum
import com.xiajun.bap.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * DW 用户注册主题
  */
class DWReleaseRegist {
}

object DWReleaseRegist {
    private val logger: Logger = LoggerFactory.getLogger(DWReleaseRegist.getClass)

    /**
      * 用户注册
      *
      * @param spark
      * @param appName
      * @param bdp_day
      */
    def handleReleaseJob(spark: SparkSession, appName: String, bdp_day: String): Unit = {
        val begin = System.currentTimeMillis()

        try {
            //导入隐式转换
            import org.apache.spark.sql.functions._
            val saveMode = SaveMode.Overwrite
            val storageLevel = ReleaseConstant.DEF_STORAGE_LEVEL

            val registColumns = DWRealseColumnsHelper.selectDWReleaseRegistColumns()

            val registCondition = (col(ReleaseConstant.DEF_PARTITION) === lit(bdp_day)
                and
                col(ReleaseConstant.COL_RELEASE_SESSION_STATUS) === lit(ReleaseStatusEnum.REGISTER.getCode))

            val registDF = SparkHelper.readTableData(spark, ReleaseConstant.ODS_RELEASE_SESSION, registColumns)
                .where(registCondition)
                .repartition(ReleaseConstant.DEF_SOURCE_PARTITION)
            SparkHelper.writeTableData(registDF, ReleaseConstant.DW_RELEASE_REGISTER_USERS, saveMode)
        } catch {
            case ex: Exception => logger.error(ex.getMessage, ex)
        } finally {
            //任务处理时长
            val msg = s"任务 ${appName}-bdp_day = ${bdp_day} 处理时长: ${System.currentTimeMillis() - begin}ms"
            logger.info(msg)
        }
    }

    def handleJobs(appName: String, bdp_day_begin: String, bdp_day_end: String): Unit = {
        var spark: SparkSession = null
        try {
            val conf = new SparkConf()
                .set("hive.exec.dynamic.partition", "true")
                .set("hive.exec.dynamic.partition.mode", "nonstrict")
                .set("spark.sql.shuffle.partitions", "32")
                .set("hive.merge.mapfiles", "true")
                .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
                .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
                .set("spark.sql.crossJoin.enabled", "true")
            spark = SparkHelper.createSparkSession(conf)

            val dateRange: mutable.Seq[String] = SparkHelper.rangDates(bdp_day_begin, bdp_day_end)

            for (elem <- dateRange) {
                handleReleaseJob(spark, appName, elem)
            }
        } catch {
            case ex: Exception => logger.error(ex.getMessage, ex)
        }
    }

    def main(args: Array[String]): Unit = {
        val appName = "dw_release_register_user"
        val bdp_day_begin = "2019-09-24"
        val bdp_day_end = "2019-09-24"
        handleJobs(appName, bdp_day_begin, bdp_day_end)
    }
}
