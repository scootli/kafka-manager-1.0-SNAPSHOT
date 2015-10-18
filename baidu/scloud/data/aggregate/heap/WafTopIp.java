package baidu.scloud.data.aggregate.heap;

/**
 * top waf ip data structure
 * @param ip: waf attack ip
 * @param attack_count: waf attack ip times
 */
public class WafTopIp extends HeapData{
	private long ip;
	private long attack_count;
	
	public WafTopIp(){
		
	}
	
	public WafTopIp(long ip,long attack_count){
		this.ip = ip;
		this.attack_count = attack_count;
	}
	
	public boolean less_override(HeapData other){
		return this.attack_count < ((WafTopIp)other).attack_count;
	}
	
	public long get_attack_count() {
		return attack_count;
	}
	
	public void set_attack_count(long attack_count) {
		this.attack_count = attack_count;
	}
	
	public long get_ip() {
		return ip;
	}
	
	public void set_ip(long ip) {
		this.ip = ip;
	}
}
