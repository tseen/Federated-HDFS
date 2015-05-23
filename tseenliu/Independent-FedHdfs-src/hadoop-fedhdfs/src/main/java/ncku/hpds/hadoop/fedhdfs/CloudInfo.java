package ncku.hpds.hadoop.fedhdfs;

import java.text.DecimalFormat;

public class CloudInfo {
	
	private long hdfsRemain;
	private long dataSize;
	private double mValue;
	private double Alpha = 0.7;

	public void setHdfs(long hdfs) {
		this.hdfsRemain = hdfs;
	}

	public void setData(long data) {
		this.dataSize = data;
	}
	
	public void setValue() {
		mValue = (Alpha*dataSize + (1-Alpha)*hdfsRemain);
	}

	protected long getHdfs() {
		return hdfsRemain;
	}

	protected long getData() {
		return dataSize;
	}
	
	protected double getValue() {
		return mValue;
	}

	public String toString() {
		return hdfsRemain + " " + dataSize + " " + new DecimalFormat("#.##").format(mValue);
	}
}
