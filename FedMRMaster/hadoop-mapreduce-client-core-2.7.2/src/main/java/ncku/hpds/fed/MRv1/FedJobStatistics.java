package ncku.hpds.fed.MRv1;
import java.util.Date;

public class FedJobStatistics {
    public long regionCloud_start = 0;
    public long regionCloud_end = 0;
    public long regionCloud_time = 0;
    public long globalAggregation_start = 0;
    public long globalAggregation_end = 0;
    public long globalAggregation_time = 0;
    public long topCloud_start = 0;
    public long topCloud_end = 0;
    public long touCloud_time = 0;
    public FedJobStatistics() {
    }
    //-----------------------------------------------------------------
    // statistics region clouds time
    public void setRegionCloudsStart() {
        regionCloud_start = new Date().getTime();
    }
    public void setRegionCloudsEnd() {
        regionCloud_end = new Date().getTime();
        regionCloud_time = this.regionCloud_end - this.regionCloud_start;
    }
    public long getRegionCloudsTime() {
        return regionCloud_time; 
    }
    public long getRegionCloudStart() { return regionCloud_start ; }
    public long getRegionCloudEnd() { return regionCloud_time ; } 
    //-----------------------------------------------------------------
    // statistics top cloud time
    public void setTopCloudStart() {
        topCloud_start = new Date().getTime();
    }
    public void setTopCloudEnd() {
        topCloud_end = new Date().getTime();
        regionCloud_time = this.topCloud_end - this.topCloud_start ;
    }
    public long getTopCloudTime() {
        return touCloud_time ; 
    }
    public long getTopCloudStart() { return topCloud_start ; }
    public long getTopCloudEnd() { return topCloud_end ; } 
    //-----------------------------------------------------------------
    // statistics global aggregation time
    public void setGlobalAggregationStart() {
        globalAggregation_start = new Date().getTime();
    }
    public void setGlobalAggregationEnd() {
        globalAggregation_end = new Date().getTime();
        globalAggregation_time = this.globalAggregation_end - this.globalAggregation_start ;
    }
    public long getGlobalAggregationTime() {
        return globalAggregation_time ; 
    }
    public long getGlobalAggregationStart() { return globalAggregation_start; }
    public long getGlobalAggregationEnd() { return globalAggregation_end ; } 
}
