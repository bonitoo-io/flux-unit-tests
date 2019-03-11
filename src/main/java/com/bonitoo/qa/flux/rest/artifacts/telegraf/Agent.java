package com.bonitoo.qa.flux.rest.artifacts.telegraf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Agent {

    int collectionInterval;

    public Agent(){
        collectionInterval = 10000;
    }

    public Agent(int collectionInterval) {
        this.collectionInterval = collectionInterval;
    }

    public int getCollectionInterval() {
        return collectionInterval;
    }

    public void setCollectionInterval(int collectionInterval) {
        this.collectionInterval = collectionInterval;
    }
}
