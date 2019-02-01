package com.bonitoo.qa.influx2;

import java.util.ArrayList;
import java.util.List;

public class Configuration {

    private String orgId;

    private List<String> bucketIds;

    private String token;

    public Configuration(){}

    public Configuration(String orgId, List<String> bucketIds, String token) {
        this.orgId = orgId;
        this.bucketIds = bucketIds;
        this.token = token;
    }

    public Configuration(String orgId, String[] bucketIds, String token) {
        this.orgId = orgId;
        this.bucketIds = new ArrayList<String>();

        for(String s : bucketIds){
            this.bucketIds.add(s);
        }

        this.token = token;
    }


    public String getOrgId() {
        return orgId;
    }

    public List<String> getBucketIds() {
        return bucketIds;
    }

    public String getToken() {
        return token;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public void setBucketIds(List<String> bucketIds) {
        this.bucketIds = bucketIds;
    }

    public void setBucketIds(String[] bucketIds) {
        this.bucketIds = new ArrayList<String>();

        for(String s : bucketIds){
            this.bucketIds.add(s);
        }
    }


    public void setToken(String token) {
        this.token = token;
    }
}
