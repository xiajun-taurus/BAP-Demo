package com.xiajun.bap.release.enums;

/**
 * 状态枚举
 */
public enum ReleaseStatusEnum {
    /**
     * 非目标客户
     */
    NOTCUSTOMER("00", "非目标客户"),
    /**
     * 目标客户
     */
    CUSTOMER("01", "目标客户"),
    BIDING("02", "竞价"),
    /**
     * 曝光
     */
    SHOW("03", "曝光"),
    /**
     * 点击
     */
    CLICK("04", "点击"),
    /**
     * 到达
     */
    ARRIVE("05", "到达"),
    /**
     * 注册
     */
    REGISTER("06", "注册(用户)");

    private String code;
    private String desc;

    ReleaseStatusEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
