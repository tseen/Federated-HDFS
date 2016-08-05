package fbicloud.utils ;


public class FGInfo
{
	// flow group ID
	private String id ;
	// flow group feature vectors
	private double[] features = new double[20] ;
	
	
	public FGInfo ( String fgid, double[] fv )
	{
		this.id = fgid ;
		this.features = fv ;
	}
	
	public String getID ()
	{
		return id ;
	}
	
	public double[] getFeatures ()
	{
		return features ;
	}
}