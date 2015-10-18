package baidu.scloud.data.aggregate.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class CommonUtils {
	/**
	 * Get yesterday time 
	 * @return
	 */
	public static int getYesterdayTime(){
		Calendar cal=Calendar.getInstance();
		cal.add(Calendar.DATE,-1);
        Date d=cal.getTime();
		java.text.SimpleDateFormat sp=new java.text.SimpleDateFormat("yyyyMMdd");
		String yesterdayStr=sp.format(d);
		return Integer.parseInt(yesterdayStr);
	}
	
	/**
	 * Get pre hour time from current time
	 * current time minus 10 minutes
	 * @return: date-hour
	 */
	public static String getPreHour(int cardinal){
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(System.currentTimeMillis() - 600000);
		calendar.set(Calendar.HOUR_OF_DAY, calendar.get(Calendar.HOUR_OF_DAY) + cardinal);
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd-HH");
		return df.format(calendar.getTime());
	}
	
	/**
	 * first char Upper
	 * @param value
	 * @return
	 */
	public static String captureName(String value){
		return value.replaceFirst(value.substring(0, 1),value.substring(0, 1).toUpperCase())  ; 
	}
	
	/**
	 * Get filename timestamp parts
	 * @param currentTimesStamp
	 * @return
	 */
	public static String TimeStamp2Date(long currentTimesStamp){
		Long timestamp = currentTimesStamp * 1L;   
		String date = new java.text.SimpleDateFormat("yyyyMMddHH").format(new java.util.Date(timestamp));    
		return date;    
	}
	
	
}
