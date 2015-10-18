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
import baidu.scloud.data.aggregate.base.HourAggregate;
import baidu.scloud.data.aggregate.client.PhoenixClient;
import baidu.scloud.data.aggregate.utils.CommonUtils;

/**
 * Hour aggregate task schedule
 */
public class AggregateHourData {
	static Logger logger = Logger.getLogger(AggregateHourData.class);
	
	public static void main(String[] args) throws Exception{
		String rootPath = System.getProperty("user.dir");
		PropertyConfigurator.configure(rootPath + "/conf/log4j.properties");
		
		//create the command line parser
		/*CommandLineParser parser = new DefaultParser();
		
		//create the Options
		Options options = new Options();
		//options.addOption("p", "help",  false, "print help for the command.");
		options.addOption( "t", "type", true, "appoint hour to aggregate type(one of attack-domain, attack-site," +
							"se-domain,se-site,visitorip-domain,visitorip-site,visitoruv-domain,visitoruv-site," +
							"access-domain,access-site,waf-domain,waf-site,cc-domain,cc-site)." );
		options.addOption( "d", "date", true, "appoint to aggregate date(for example: 20150830)" );
		options.addOption( "h", "help", false, "print help for the command");
		options.addOption( "u", "hour", true, "appoint to aggregate hour(for example: 10)");
		
		String type = "";
		int date;
		int hour;
		try {
		    // parse the command line arguments
		    CommandLine line = parser.parse( options, args );
		    // validate that type has been set
		    if(!line.hasOption("t")) {
				String formatstr = "AggregateHourData [-t/--type] AggregateType [-d/--date][-u/--hour][-h/--help]";
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp( formatstr, options );
				return;
		    }
			type = line.getOptionValue("t");
			if(line.hasOption("d") && !line.hasOption("u")){
				logger.error("Please input -u Option.");
				throw new Exception("Please input -u Option.");
			}
			if(line.hasOption("d")){
				date = Integer.parseInt(line.getOptionValue("d"));
				hour = Integer.parseInt(line.getOptionValue("u"));
			}else{
				String[] preHour =  CommonUtils.getPreHour(-1).split("-");
				date = Integer.parseInt(preHour[0].trim());
				hour = Integer.parseInt(preHour[1].trim());
			}
			if(hour < 0 || hour > 23){
				logger.error("hour cann't greater 23 or less than 0");
				throw new Exception("hour cann't greater 23 or less than 0");
			}
		}catch(ParseException exp ) {
			logger.error("in AggregateHourData,Param parse exception:" + exp);
			throw exp;
		}*/
		if(args.length < 1){
			logger.error("The first param must be one of attack-domain, attack-site,se-domain,se-site,visitorip-domain," +
			"visitorip-site,visitoruv-domain,visitoruv-site,access-domain,access-site,waf-domain,waf-site,cc-domain,cc-site");
			throw new Exception("The first param error");
		}
		String type = args[0].trim();
		int date = 0;
		int hour = 0;
		try{
			int tmpDate = 0;
			if(args.length > 1){
				tmpDate = Integer.parseInt(args[1].trim());
				if(tmpDate > 0){
					date = tmpDate;
					hour = Integer.parseInt(args[2].trim());
				}
			}
			if(tmpDate <= 0){
				String[] preHour =  CommonUtils.getPreHour(tmpDate).split("-");
				date = Integer.parseInt(preHour[0].trim());
				hour = Integer.parseInt(preHour[1].trim());
			}
			if(hour < 0 || hour > 23){
				logger.error("hour cann't greater 23 or less than 0");
				throw new Exception("hour cann't greater 23 or less than 0");
			}
		}catch(Exception ex){
			throw new Exception("The first param is type. The second param is date(-1,-2,20150902),The thrid param is hour. Exception: " + ex);
		}
		
		
		AbstractFileConfiguration config = new PropertiesConfiguration();
		try {
			//disable comma as delimiter when prase zk_server config item
			config.setDelimiterParsingDisabled(true);
			config.load("conf/hindex.properties");
		} catch (ConfigurationException ex) {
			logger.error("load hindex config file failed!");
			throw ex;
		}
		
		PhoenixClient phoenixClient = new PhoenixClient(config);
		
		//execute corresponding operator in accord to param
		logger.info("start " + type + " task successfully! date: " + date + " hour: " + hour);
		String[] typeArr = type.split("-");
		String aggrType = typeArr[0];
		String aggrRange = typeArr[1];
		String driverClass = "baidu.scloud.data.aggregate." + aggrType + "." + CommonUtils.captureName(aggrType)
											+ "HourAggregate";
		//ClassLoader with reflect mechanism
		try {
			Class<?> cls = Class.forName(driverClass);
			Constructor cons = cls.getConstructor(phoenixClient.getClass());
			HourAggregate hourAggr = (HourAggregate)cons.newInstance(phoenixClient);
			
			int result = 0;
			if(aggrRange.equalsIgnoreCase("domain")){
				result = hourAggr.summaryHourDomainData(date,hour);
			}else if(aggrRange.equalsIgnoreCase("site")){
				result = hourAggr.summaryHourSiteData(date,hour);
			}
			
			if(result > 0){
				throw new Exception("Current task failed. please check phoenix or mysql connection error log. date: " +  date + " hour: " + hour);
			}
		} catch (Exception ex) {
			logger.error("in AggregateHourData,Please check param. driverClass: " 
					+ driverClass + " Exception: " + ex);
			throw ex;
		}
		
		logger.info("current task execute finish. date: " +  date + " hour: " + hour);
	}
}
