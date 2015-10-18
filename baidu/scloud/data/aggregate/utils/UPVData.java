package baidu.scloud.data.aggregate.utils;

/**
 * tools class for uv and pv from cf_rt_visitor_uv_v2
 * @param uv: from cf_rt_visitor_uv_v2 uv_key field
 * @param pv: from cf_rt_visitor_uv_v2 pv_key field
 */
public class UPVData {
	private long uv;
	private long pv;
	
	public UPVData(long uv_num,long pv_num){
		this.uv = uv_num;
		this.pv = pv_num;
	}

	public long getPv() {
		return pv;
	}
	

	public void setPv(long pv) {
		this.pv = pv;
	}
	

	public long getUv() {
		return uv;
	}
	

	public void setUv(long uv) {
		this.uv = uv;
	}
}
