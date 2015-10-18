package baidu.scloud.data.aggregate.heap;

/**
 * visitor page data structure
 * @param uri: visitor page uri
 * @param req_pv_count: visitor page uri request count
 */
public class VisitorPage extends HeapData{
	private String uri;
	private long req_pv_count;
	
	public VisitorPage(){
		
	}
	
	public VisitorPage(String uri,long req_pv_count){
		this.uri = uri;
		this.req_pv_count = req_pv_count;
	}
	
	public boolean less_override(HeapData other){
		return this.req_pv_count < ((VisitorPage)other).req_pv_count;
	}
	
	public long get_req_pv_count() {
		return req_pv_count;
	}
	
	public void set_req_pv_count(long req_pv_count) {
		this.req_pv_count = req_pv_count;
	}
	
	public String get_uri() {
		return uri;
	}
	
	public void set_uri(String uri) {
		this.uri = uri;
	}
}
