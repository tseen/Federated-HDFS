package ncku.hpds.hadoop.fedhdfs;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class PathInfo implements Serializable {
	
	SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm");

	private String path;
	
	private long length;
	
	private long modificationTime;
	
	private short replication;
	
	private String owner;
	
	private String group;
	
	private String permission;
	
	public String getPath()
    {
    	return path;
    }

    public long getLength()
    {
        return length;
    }

    public long getModificationTime()
    {
        return modificationTime;
    }

    public short getReplication()
    {
        return replication;
    }

    public String getOwner()
    {
        return owner;
    }

    public String getGroup()
    {
        return group;
    }
    
    public String getPermission()
    {
        return permission;
    }
	
	/**/
	
    public void setPath(String path)
    {
    	this.path = path;
    }

    public void setLength(long length)
    {
        this.length = length;
    }

    public void setModificationTime(long modificationTime)
    {
        this.modificationTime = modificationTime;
    }

    public void setReplication(short replication)
    {
        this.replication = replication;
    }

    public void setOwner(String owner)
    {
        this.owner = owner;
    }

    public void setGroup(String group)
    {
        this.group = group;
    }
    
    public void setPermission(String permission)
    {
        this.permission = permission;
    }
    
    public String toString() {
        return  permission
        		+","
				+ replication
				+ ","
				+ owner
				+ ","
				+ group
				+ ","
				+ length
				+ ","
				+ f.format(new Timestamp(modificationTime))
				+ ","
				+ path;

    }

}
