package fbicloud.utils ;

import java.util.Collection ;

public class FVInfo
{
	int ID ;
	Collection<String> IPsCol ;
	
	public FVInfo ( int fvID, Collection<String> fvIPs )
	{
		this.ID = fvID ;
		this.IPsCol = fvIPs ;
	}
	
	public int getID()
	{
		return ID ;
	}
	
	public Collection<String> getIPsCol()
	{
		return IPsCol ;
	}
}
