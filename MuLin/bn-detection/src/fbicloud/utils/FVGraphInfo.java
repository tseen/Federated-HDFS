package fbicloud.utils ;


public class FVGraphInfo
{
	private String id ;
	private String neighbor ;
	
	public FVGraphInfo ( String id, String neighbor )
	{
		this.id = id ;
		this.neighbor = neighbor ;
	}
	
	public String getID()
	{
		return id ;
	}
	
	public String getNeighbor()
	{
		return neighbor ;
	}
}