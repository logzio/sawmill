package io.logz.sawmill.conditions;

import io.logz.sawmill.Condition;
import io.logz.sawmill.Doc;
import io.logz.sawmill.annotations.ConditionProvider;
import io.logz.sawmill.parser.ConditionParser;
import io.logz.sawmill.utilities.JsonUtils;

import java.net.InetAddress; 
import java.net.UnknownHostException; 
import java.util.Map;

import static java.util.Objects.requireNonNull;

@ConditionProvider(type = "ipRange", factory = IpCompareCondition.Factory.class)
public class IpCompareCondition implements Condition  {
	
	private String ipHigh;
	private String ipLow;
	private String ipTest;
	
	public IpCompareCondition(String ipTest, String ipLow, String ipHigh) {
		this.ipTest = requireNonNull(ipTest);
		this.ipHigh = requireNonNull(ipHigh);
		this.ipLow = requireNonNull(ipLow);
		
	}
	
	public static long ipToLong(InetAddress ip) {
        byte[] octets = ip.getAddress();
        long result = 0;
        for (byte octet : octets) {
            result <<= 8;
            result |= octet & 0xff;
        }
        return result;
    }
	
    @Override
    public boolean evaluate(Doc doc) {
        
	if (!doc.hasField(ipTest, String.class)) return false;
        String value = (doc.getField(this.ipTest));

        long ipLo = 0;
		try {
			ipLo = ipToLong(InetAddress.getByName(ipLow));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        long ipHi = 0;
		try {
			ipHi = ipToLong(InetAddress.getByName(ipHigh));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        long ipToTest = 0;
		try {
			ipToTest = ipToLong(InetAddress.getByName(value));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        return ipToTest >= ipLo && ipToTest <= ipHi;
    }
    
    public static class Factory implements Condition.Factory {

        public Factory() {}

        @Override
        public IpCompareCondition create(Map<String, Object> config, ConditionParser conditionParser) {
        	IpCompareCondition.Configuration configuration = JsonUtils.fromJsonMap(IpCompareCondition.Configuration.class, config);
            return new IpCompareCondition(
            		configuration.getIpTest(),
                    configuration.getIpLow(),
                    configuration.getIpHigh());
        }

    }

    public static class Configuration {


    	private String ipHigh;
    	private String ipLow;
    	private String ipTest;

        public Configuration() {}
        
        public Configuration(String ipTest, String ipLow, String ipHigh) {
    		this.ipTest = ipTest;
    		this.ipHigh = ipHigh;
    		this.ipLow = ipLow;
        }

        public String getIpHigh() {
            return ipHigh;
        }

        public String getIpLow() {
            return ipLow;
        }

        public String getIpTest() {
            return ipTest;
        }

    }

}
