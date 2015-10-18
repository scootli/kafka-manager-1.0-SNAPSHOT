package baidu.scloud.data.aggregate.schedule;

import java.lang.reflect.Constructor;
/*import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;*/
import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import baidu.scloud.data.aggregate.base.DayAggregate;
import baidu.scloud.data.aggregate.utils.CommonUtils;
import baidu.scloud.data.aggregate.utils.IPInfo;

/**
 * Hour aggregate task schedule
 */
public class AggregateDayData {
	static Logger logger = Logger.getLogger(AggregateDayData.class);
	
	public static void main(String[] args) throws Exception {
		String rootPath = System.getProperty("user.dir");
		PropertyConfigurator.configure(rootPath + "/conf/log4j.properties");
		
		//create the command line parser
		/*CommandLineParser parser = new DefaultParser();
		
		//create the Options
		Options options = new Options();
		//options.addOption("p", "help",  false, "print help for the command.");
		options.addOption( "t", "type", true, "appoint hour to aggregate type(one of visitorpage-domain," +
				"visitorpage-site,wafip-domain,wafip-site)." );
		options.addOption( "d", "date", true, "appoint to aggregate date(for example: 20150830)" );
		options.addOption( "p", "top", true, "get topk limit result. for example: top 10" );
		options.addOption( "h", "help", false, "print help for the command");
		
		String type = "";
		int date;
		int topk = 5;
		try {
		    // parse the command line arguments
		    CommandLine line = parser.parse( options, args );
		    // validate that type has been set
		    if(!line.hasOption("t") || line.hasOption("h")) {
				String formatstr = "AggregateDayData [-t/--type] AggregateType [-d/--date][-p/--top][-h/--help]";
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp( formatstr, options );
				return;
		    }
			type = line.getOptionValue("t");
			if(line.hasOption("d")){
				date = Integer.parseInt(line.getOptionValue("d"));
			}else{
				date = CommonUtils.getYesterdayTime();
			}
			if(line.hasOption("p")){
				topk = Integer.getInteger(line.getOptionValue("p"));
			}
		}catch(ParseException exp ) {
			logger.error("in AggregateDayData,Param parse exception:" + exp);
			throw exp;
		}*/
		
		if(args.length < 1){
			logger.error("The first param must be one of visitorpage-domain,visitorpage-site,wafip-domain,wafip-site");
			throw new Exception("The first param error");
		}
		String type = args[0].trim();
		int date;
		int topk;
		try{
			if(args.length > 1){
				date = Integer.parseInt(args[1].trim());
				topk = Integer.parseInt(args[2].trim());
			}else{
				date = CommonUtils.getYesterdayTime();
				topk = 5;
			}
			
			if(date < 0 || topk < 0 || topk > 20){
				throw new Exception("date < 0 or topk < 0 or topk > 20 error");
			}
		}catch(Exception ex){
			throw new Exception("The first param is type. The second param is date(20150902),The thrid param is topk(5). Exception: " + ex);
		}
		
		AbstractFileConfiguration config = new PropertiesConfiguration();
		try {
			//disable comma as delimiter when prase zk_server config item
			config.setDelimiterParsingDisabled(true);
			config.load("conf/hindex.properties");
		} catch (ConfigurationException ex) {
			logger.error("load hindex mysql config file faied! Exception: " + ex);
			throw ex;
		}
		
		//execute corresponding operator in accord to param
		logger.info("start " + type + " task successfully! date: " + date + " topk: " + topk);
		String[] typeArr = type.split("-");
		String aggrType = typeArr[0];
		String aggrRange = typeArr[1];
		String driverClass = "baidu.scloud.data.aggregate." + aggrType + "." + CommonUtils.captureName(aggrType)
											+ "DayAggregate";
		
		//ClassLoader with reflect mechanism
		try {
			Class<?> cls = Class.forName(driverClass);
			Constructor cons = cls.getConstructor(config.getClass());	
			DayAggregate dayAggr = (DayAggregate)cons.newInstance(config);
			
			if(aggrType.equalsIgnoreCase("wafip")){
				String fileName = "conf/IP/colombo_iplib.txt";
				IPInfo.loadIpMapFile(fileName);
			}
			
			int result = 0;
			if(aggrRange.equalsIgnoreCase("domain")){
				result = dayAggr.summaryDayDomainData(date,topk);
			}else if(aggrRange.equalsIgnoreCase("site")){
				result = dayAggr.summaryDaySiteData(date,topk);
			}
			if(result > 0){
				throw new Exception("Current task failed. please check phoenix or mysql connection error log. date: " +  date + " topk: " + topk);
			}
		} catch (Exception ex) {
			logger.error("in AggregateDayData,Please check param. driverClass: " 
					+ driverClass + " Exception: " + ex);
			throw ex;
		}
		logger.info("current task execute finish. date: " +  date + " topk: " + topk);
		
	}
}
