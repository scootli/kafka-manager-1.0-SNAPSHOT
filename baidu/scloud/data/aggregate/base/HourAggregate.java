package baidu.scloud.data.aggregate.base;

/**
 * Hour Aggregate interface
 */
public interface HourAggregate {
	/**
	 * Aggregate domain data
	 */
	public int summaryHourDomainData(int date,int hour);
	/**
	 * Aggregate site data
	 */
	public int summaryHourSiteData(int date,int hour);
}
