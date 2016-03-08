/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;
import java.util.Date;

public class FedJobStatistics {
    public int regionCloud_start_flag = 0;
    public long regionCloud_start = 0;
    public long regionCloud_end = 0;
    public long regionCloud_time = 0;
    public long globalAggregation_start = 0;
    public long globalAggregation_end = 0;
    public long globalAggregation_time = 0;
    public long topCloud_start = 0;
    public long topCloud_end = 0;
    public long topCloud_time = 0;
    public FedJobStatistics() {
    }
    public void setRegionCloudFlag(){
        regionCloud_start_flag = 1;
    }
    //-----------------------------------------------------------------
    // statistics region clouds time
    public void setRegionCloudsStart() {
        regionCloud_start = System.currentTimeMillis();
    }
    public void setRegionCloudsEnd() {
        regionCloud_end = System.currentTimeMillis();
        regionCloud_time = this.regionCloud_end - this.regionCloud_start;
    }
    public long getRegionCloudsTime() {
        return regionCloud_time; 
    }
    public long getRegionCloudsStart() { return regionCloud_start ; }
    public long getRegionCloudsEnd() { return regionCloud_time ; } 
    //-----------------------------------------------------------------
    // statistics top cloud time
    public void setTopCloudStart() {
        topCloud_start = System.currentTimeMillis();
    }

    public void setTopCloudEnd() {
        topCloud_end = System.currentTimeMillis();
        topCloud_time = this.topCloud_end - this.topCloud_start ;
    }
    public long getTopCloudTime() {
        return topCloud_time ; 
    }
    public long getTopCloudStart() { return topCloud_start ; }
    public long getTopCloudEnd() { return topCloud_end ; } 
    //-----------------------------------------------------------------
    // statistics global aggregation time
    public void setGlobalAggregationStart() {
        globalAggregation_start = System.currentTimeMillis();
    }
    public void setGlobalAggregationEnd() {
        globalAggregation_end = System.currentTimeMillis();
        globalAggregation_time = this.globalAggregation_end - this.globalAggregation_start ;
    }
    public long getGlobalAggregationTime() {
        return globalAggregation_time ; 
    }
    public long getGlobalAggregationStart() { return globalAggregation_start; }
    public long getGlobalAggregationEnd() { return globalAggregation_end ; } 
}
