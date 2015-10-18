package baidu.scloud.data.aggregate.base;

/**
 * Day Aggregate interface
 */
public interface DayAggregate {
	/**
	 * Aggregate domain data
	 */
	public int summaryDayDomainData(int date,int topk);
	/**
	 * Aggregate site data
	 */
	public int summaryDaySiteData(int date,int topk);
}
