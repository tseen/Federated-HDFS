/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
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
    public static final String REQ_REGION_MAP_FINISHED = "Region-Map-Finished";
    public static final String RES_REGION_MAP_FINISHED = "OK";
    public static final String REQ_REGION_WAN = "Region-WAN";
    public static final String RES_REGION_WAN = "WOK";
	public static final String REQ_INFO = "req-WAN-Infos";
	public static final String REQ_INFO_2 = "req-realValue";

	public static final String RES_INFO = "res-WAN-Infos";
	public static final String REQ_REGION_RESOURCE = "req-reg-resrc";
	public static final String RES_REGION_RESOURCE = "res-reg-resrc";
	public static final String REQ_INTER_SIZE = "req-inter-size";
	public static final String RES_INTER_SIZE = "res-inter-size";
	public static final String REQ_INTER_INFO = "req-inter-info";
	public static final String RES_INTER_INFO = "res-inter-info";
	public static final String REQ_WAIT_BARRIER = "req-barrier";
	public static final String RES_TRUE_BARRIER = "res-barrier-true";
	public static final String RES_FALSE_BARRIER = "res-barrier-false";
	public static final String REQ_RM_START = "region-start";
	public static final String REQ_INTER_START = "inter--start";
	public static final String REQ_INTER_STOP = "inter---stop";
	public static final String REQ_TOP_START = "top-start";
	public static final String REQ_TOP_STOP = "top--stop";
	public static final String REQ_RM_STOP = "region--stop";






    public enum FedSocketState {
        NONE, ACCEPTING, ACCEPTED, CONNECTING, CONNECTED, DISCONNECTED
    };
}
