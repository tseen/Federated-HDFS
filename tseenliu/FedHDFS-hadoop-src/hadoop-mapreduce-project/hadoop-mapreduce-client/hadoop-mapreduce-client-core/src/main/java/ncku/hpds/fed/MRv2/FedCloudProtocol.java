package ncku.hpds.fed.MRv2;

public class FedCloudProtocol {
    /*
     *   Client                 Server
     *   Top Cloud   <---->      Region Cloud
     *    req PING                 res PONG
     *    res OK                   req Map-ProxyReduce Finished
     *    res OK                   req Start-Local Aggregation ( upload partial result from region cloud to top cloud )
     *    record AggregationTime   req Upload Finished  ( if not iterative, Top Cloud disconnect and Server Closed )
     *
     */
	public static final String REQ_MAP_PROXY_REDUCE_FINISHED = "MAP-PR-Finished";
    public static final String REQ_MIGRATE_DATA = "Migrate-Data";
    public static final String REQ_MIGRATE_DATA_FINISHED = "Migrate-Data-Finished";
	public static final String REQ_PING = "PING";
	public static final String RES_PONG = "PONG";
    public static final String RES_OK = "OK";
    public static final String REQ_BYE = "BYE";
    public static final String RES_BYE = "GoodBYE";
    public enum FedSocketState {
        NONE, ACCEPTING, ACCEPTED, CONNECTING, CONNECTED, DISCONNECTED
    };
}
