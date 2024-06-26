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
	
	private final String ipHigh;
	private final String ipLow;
	private final String field;
	
	public IpCompareCondition(String field, String ipLow, String ipHigh) {
		this.field = requireNonNull(field);
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
        
	if (!doc.hasField(field, String.class)) return false;
        String value = (doc.getField(this.field));

        long ipLo = 0;
		try {
			ipLo = ipToLong(InetAddress.getByName(ipLow));
		} catch (UnknownHostException e) {
			//throw new RuntimeException(String.format("Invalid ip address [%s]", ipLow), e);
		}
        long ipHi = 0;
		try {
			ipHi = ipToLong(InetAddress.getByName(ipHigh));
		} catch (UnknownHostException e) {
			//throw new RuntimeException(String.format("Invalid ip address [%s]", ipHigh), e);
		}
        long ipToTest = 0;
		try {
			ipToTest = ipToLong(InetAddress.getByName(value));
		} catch (UnknownHostException e) {
			//throw new RuntimeException(String.format("Invalid ip address [%s]", value), e);
		}
        
        return ipToTest >= ipLo && ipToTest <= ipHi;
    }
    
    public static class Factory implements Condition.Factory {

        public Factory() {}

        @Override
        public IpCompareCondition create(Map<String, Object> config, ConditionParser conditionParser) {
        	IpCompareCondition.Configuration configuration = JsonUtils.fromJsonMap(IpCompareCondition.Configuration.class, config);
            return new IpCompareCondition(
            		configuration.getField(),
                    configuration.getIpLow(),
                    configuration.getIpHigh());
        }

    }

    public static class Configuration {


    	private String ipHigh;
    	private String ipLow;
    	private String field;

        public Configuration() {}
        
        public Configuration(String field, String ipLow, String ipHigh) {
    		this.field = field;
    		this.ipHigh = ipHigh;
    		this.ipLow = ipLow;
        }

        public String getIpHigh() {
            return ipHigh;
        }

        public String getIpLow() {
            return ipLow;
        }

        public String getField() {
            return field;
        }

    }

}
