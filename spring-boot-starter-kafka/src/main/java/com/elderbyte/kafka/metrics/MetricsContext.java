package com.elderbyte.kafka.metrics;

public class MetricsContext {

    /***************************************************************************
     *                                                                         *
     * Static builders                                                         *
     *                                                                         *
     **************************************************************************/

    public static MetricsContext from(String appId, String instanceId){
        return new MetricsContext(appId, instanceId);
    }

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final String appId;
    private final String instanceId;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/


    private MetricsContext(String appId, String instanceId){
        this.appId = appId;
        this.instanceId = instanceId;
    }

    /***************************************************************************
     *                                                                         *
     * Properties                                                              *
     *                                                                         *
     **************************************************************************/

    public String getAppId() {
        return appId;
    }

    public String getInstanceId() {
        return instanceId;
    }
}
