package fbicloud.algorithm.classes;


public class NetflowRecord
{
	// First key -> hash
	private String prot ;
	private String srcIP ;
	private String dstIP ;
	private String srcPort ;
	private String dstPort ;
	// Second key -> time
	private long timestamp ;
	// Value
	private long pkts ;
	private long bytes ;
	private long duration ;
	
	public NetflowRecord()
	{
	}
	
	// set attributes of netflow object
	public void parseRaw ( String raw )
	{
		// value :									index
		// timestamp    duration    protocol		0~2
		// srcIP    srcPort    dstIP    dstPort		3~6
		// packets    bytes							7~8
		
		String[] tokens = raw.split("\\s+") ;
		
		setInitial ( tokens[2], tokens[3], tokens[5], tokens[4], tokens[6], tokens[7], tokens[8], tokens[0], tokens[1] ) ;
	}
	
	// set attributes of netflow object
	public void setInitial ( String tmpProt, String tmpSrcIP, String tmpDstIP, String tmpSrcPort, String tmpDstPort,
							 String tmpPkts, String tmpBytes, String tmpTimestamp, String tmpDuration )
	{
		this.prot = tmpProt ;
		this.srcIP = tmpSrcIP ;
		this.dstIP = tmpDstIP ;
		this.srcPort = tmpSrcPort ;
		this.dstPort = tmpDstPort ;
		this.pkts = Long.parseLong(tmpPkts) ;
		this.bytes = Long.parseLong(tmpBytes) ;
		this.timestamp = Long.parseLong(tmpTimestamp) ;
		this.duration = Long.parseLong(tmpDuration) ;
	}
	
	public String getKey ()
	{
		if ( srcIP.compareTo(dstIP) <= 0 )
		{
			// test
			//System.out.println( "compareTo <= 0" ) ;
			return prot + "-" + srcIP + ":" + srcPort + "-" + dstIP + ":" + dstPort ;
		}
		else
		{
			// test
			//System.out.println( "compareTo > 0" ) ;
			return prot + "-" + dstIP + ":" + dstPort + "-" + srcIP + ":" + srcPort ;
		}
	}
	
	public String getCompare ()
	{
		if(srcIP.compareTo(dstIP)<=0){
			return "+";//source destination do not switch in getKey()
		}
		else{
			return "-";//source destination have been switch in getKey()
		}
	}
	
	public String getProt()
	{
		return prot ;
	}
	public String getSrcIP()
	{
		return srcIP ;
	}
	public String getDstIP()
	{
		return dstIP ;
	}
	public String getSrcPort()
	{
		return srcPort ;
	}
	public String getDstPort()
	{
		return dstPort ;
	}
	public long getPkts()
	{
		return pkts ;
	}
	public long getBytes()
	{
		return bytes ;
	}
	public long getTimestamp()
	{ 
		return timestamp ;
	}
	public long getDuration()
	{ 
		return duration ;
	}
}
