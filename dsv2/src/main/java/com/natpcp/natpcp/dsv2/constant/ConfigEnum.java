package com.natpcp.natpcp.dsv2.constant;

/**
 * @Auther wangl
 * @Date : 2024/3/15  10:12
 */
public enum ConfigEnum {
    /**
     * logLevel
     */
    LOG_LEVEL("logLevel"),
    /**
     *jvmMemory
     */
    JVM_MEMORY("jvmMemory"),
    /**
     *jvmThread
     */
    JVM_THREAD("jvmThread"),
    /**
     *redis
     */
    REDIS("redis"),
    /**
     *rocketMQ
     */
    ROCKET_MQ("rocketMQ"),
    /**
     *postgres
     */
    POSTGRES("postgres"),
    /**
     *dataSource
     */
    DATASOURCE("dataSource");

    public String name;

    ConfigEnum(String name) {
        this.name = name;
    }

    ConfigEnum() {
    }

    public static ConfigEnum getByName(String name){
        for (ConfigEnum configEnum : ConfigEnum.values()) {
            if (configEnum.name.equals(name)) {
                return configEnum;
            }
        }
        return null;
    }
}
