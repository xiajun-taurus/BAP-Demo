package com.xiajun.bap.release.constant

import org.apache.spark.storage.StorageLevel

/**
  * 常量
  */
object ReleaseConstant {
    //patition缓存级别
    val DEF_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK

    //分区字段
    val DEF_PARTITION = "bdp_day"
    //重分区数
    val DEF_SOURCE_PARTITION = 4
    //维度列
    val COL_RELEASE_SESSION_STATUS = "release_status"

    //----ods----
    val ODS_RELEASE_SESSION = "ods_release.ods_01_release_session"

    //----dw----
    val DW_RELEASE_CUSTOMER = "dw_release.dw_release_customer"

    val DW_RELEASE_EXPOSURE = "dw_release.dw_release_exposure"

    val DW_RELEASE_CLICK = "dw_release.dw_release_click"

    val DW_RELEASE_REGISTER_USERS = "dw_release.dw_release_register_users"
}
