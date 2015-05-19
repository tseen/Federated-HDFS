package ncku.hpds.hadoop.fedhdfs;

public class CloudInfo {
	
	private long hdfsRemain;
	private long dataSize;

	public void setHdfs(long hdfs) {
		this.hdfsRemain = hdfs;
	}

	public void setData(long data) {
		this.dataSize = data;
	}

	protected long getHdfs() {
		return hdfsRemain;
	}

	protected long getData() {
		return dataSize;
	}

	public String toString() {
		return hdfsRemain + " " + dataSize;
	}
}
