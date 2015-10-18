package baidu.scloud.data.aggregate.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Get country or province from the ip address
 */
public class IPInfo {
	static Logger logger = Logger.getLogger(IPInfo.class);
	
	//ip segment upper
	public static List<Long> upper_range_list = new ArrayList<Long>();
	public static Map<Long,String> upper_range_map = new HashMap<Long,String>();
	public static int len_upper_range_list = 0;
	
	/**
	 * read file and parse ip
	 * @param fileName
	 */
	public static void loadIpMapFile(String fileName){
		String last_country = "";
		String last_province = "";
		long last_upper_ip = 0;
		try {
			FileReader reader = new FileReader(fileName);
			BufferedReader br = new BufferedReader(reader);
			String line = null;
			while((line = br.readLine()) != null) {
				if(line.startsWith("#")){
					continue;
				}
				String[] ip_line_array = line.split("\\|");
				long upper_ip  = StructUnpackI(ipToBytesByInet(ip_line_array[1]));
				
				String curr_country = ip_line_array[2].trim();
				String cur_province = "";
				
				if(curr_country.equalsIgnoreCase("CN")){
		            cur_province = new String(ip_line_array[4].getBytes(),"utf-8");
		            if(cur_province.equalsIgnoreCase("None")){
		                cur_province = "其他";
		            }
				}else{
		           curr_country = "国外";
				}
				
				//keep union with pre item
				if(curr_country.equalsIgnoreCase(last_country) && cur_province.equalsIgnoreCase(last_province)){
					if(len_upper_range_list > 0){
						upper_range_list.remove(len_upper_range_list - 1);
						upper_range_list.add(upper_ip);
					}
					if(upper_range_map.containsKey(last_upper_ip)){
						upper_range_map.remove(last_upper_ip);
						upper_range_map.put(upper_ip,curr_country + "-" + cur_province);
					}
					
					//Note
	                last_upper_ip = upper_ip;
				}else{
					upper_range_list.add(upper_ip);
					upper_range_map.put(upper_ip,curr_country + "-" + cur_province);
					len_upper_range_list++;
					
					last_country = curr_country;
		            last_province = cur_province;
		            last_upper_ip = upper_ip;
				}
			}
			
			br.close();
			reader.close();
		} catch (IOException ex) {
			logger.error("in IPInfo,loadIpMapFile failed. file name is: " + fileName + " Exception is: " + ex);
		}
	}
	
	/**
	 * Get region with string ip
	 * @param ip
	 * @return region
	 */
	public static String getIpRegion(long ip) throws Exception{
		int low = 0;
        int high = len_upper_range_list;
        while(low <= high){
            int mid = (low + high) / 2;
            long mid_value = upper_range_list.get(mid);
            if(ip < mid_value){
                high = mid -1;
            }else if(ip > mid_value){
                low = mid + 1;
            }else{
                return getRegionFromTuple(upper_range_map.get(mid_value));
            }
        }
		return getRegionFromTuple(upper_range_map.get(upper_range_list.get(low)));
	}
	
	public static String getRegionFromTuple(String region_tuple){
		String[] region_Arr = region_tuple.split("-");
	    if(region_Arr[0].equalsIgnoreCase("CN")){
	        return region_Arr[1];
	    }else{
	        return region_Arr[0];
	    }
	}
	
	/**
	 * from byte array ip to int ip 
	 * @param bytes
	 * @return
	 * @throws IOException
	 */
	public static long StructUnpackI(byte[] bytes){
		if(bytes.length != 4)
			return 0;
		long resultValue = 0;
		long mask=0xff;  
		resultValue |= (bytes[3]&mask);
		resultValue |= (bytes[2]&mask) << 8;
		resultValue |= (bytes[1]&mask) << 16;
		resultValue |= (bytes[0]&mask) << 24;
		return resultValue;
	}
	
	/**
	 * from string ip to byte array int
	 * @param ipAddr
	 * @return
	 */
	public static byte[] ipToBytesByInet(String ipAddr) {
        try {
            return InetAddress.getByName(ipAddr).getAddress();
        } catch (Exception e) {
            throw new IllegalArgumentException(ipAddr + " is invalid IP");
        }
    }
	
	public static void main(String[] args) {
		String rootPath = System.getProperty("user.dir");
		String fileName = rootPath + "/conf/IP/colombo_iplib.txt";
		System.out.println(fileName);
		IPInfo.loadIpMapFile(fileName);
		System.out.println(IPInfo.upper_range_list.get(10000));
		String str_ip = "1.0.127.255";
		try {
			System.out.println(IPInfo.getIpRegion((int)IPInfo.StructUnpackI(ipToBytesByInet(str_ip))));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
