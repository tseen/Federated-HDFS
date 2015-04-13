package ncku.hpds.hadoop.fedhdfs;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class PathInfo implements Serializable {
	
	SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm");

	private transient Path path;
	
	private long length;
	
	private long modificationTime;
	
	private short replication;
	
	private String owner;
	
	private String group;
	
	private transient FsPermission permission;
	

    public void setPath(Path path)
    {
    	this.path = path;
    }

    public void setLength(long length)
    {
        this.length = length;
    }

    public void SetModificationTime(long modificationTime)
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
    
    public void SetPermission(FsPermission permission)
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
