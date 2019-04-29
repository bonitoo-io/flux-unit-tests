package com.bonitoo.qa.influx2;

import org.influxdata.query.FluxRecord;
import org.influxdata.query.FluxTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FluxUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FluxUtils.class);

    private static Double epsilon = 0.001;

    public static boolean tablesContainsRecordWithVal(List<FluxTable> tables, String value){

        for(FluxTable table : tables){
            for(FluxRecord rec : table.getRecords()){
                if(rec.getValue().equals(value)){
                    return true;
                }
            }
        }

        LOG.warn(String.format("Failed to match value of %s", value));

        return false;
    }

    public static boolean tablesContainsRecordWithVal(List<FluxTable> tables, Double value){

        for(FluxTable table : tables){
            for(FluxRecord rec : table.getRecords()){

                if(Math.abs((Double)rec.getValue() - value) < epsilon ){
                    return true;
                }
            }
        }

        LOG.warn(String.format("Failed to match value of %f", value));

        return false;
    }

    public static boolean tablesContainsRecordWithVal(List<FluxTable> tables, Long value){

        for(FluxTable table : tables){
            for(FluxRecord rec : table.getRecords()){
                if((Long)rec.getValue() == value){
                    return true;
                }
            }
        }

        LOG.warn(String.format("Failed to match value of %d", value));

        return false;
    }

    public static boolean tablesContainsRecordsWithValsInOrder(List<FluxTable> tables, Double[] vals){

        for(FluxTable table : tables){

            int valsCt = 0;
            int matchCt = 0;

            for(FluxRecord rec : table.getRecords()){

                if(Math.abs((Double)rec.getValue() - vals[valsCt++]) < epsilon){
                    matchCt++;
                }
            }

            if(matchCt == valsCt){
                return true;
            }

        }

        String format = "Failed to match values of [";

        for(Double d : vals){
            format += d + ", " ;
        }

        format += "] ";

        LOG.warn(String.format(format));

        return false;

    }


}
