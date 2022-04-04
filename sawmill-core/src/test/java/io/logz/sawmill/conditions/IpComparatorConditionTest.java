package io.logz.sawmill.conditions;

import io.logz.sawmill.Doc;
import io.logz.sawmill.parser.ConditionParser;
import io.logz.sawmill.utils.FactoryUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static org.assertj.core.api.Assertions.assertThat;

public class IpComparatorConditionTest {
	
	public ConditionParser conditionParser;

    @Before
    public void init() {
        conditionParser = new ConditionParser(FactoryUtils.defaultConditionFactoryRegistry);
    }
    
    @Test
    public void testIps() {
        String field = "field1";
        String ipHigh = "192.200.3.0";
        String ipLow = "192.100.3.0";


        Map<String, Object> config = createConfig("field", field,
                "ipHigh", ipHigh, "ipLow", ipLow );
        IpCompareCondition ipComparatorCondition = new IpCompareCondition.Factory().create(config, conditionParser);

        Doc doc = createDoc("field1", "192.150.3.0");
        assertThat(ipComparatorCondition.evaluate(doc)).isTrue();

        doc = createDoc("field1", "192.300.3.0");
        assertThat(ipComparatorCondition.evaluate(doc)).isFalse();

        doc = createDoc("field1", "192.50.3.0");
        assertThat(ipComparatorCondition.evaluate(doc)).isFalse();
    }


    public void testIncorrectIpHighOrLowInput() {
        String field = "field1";
        String ipHigh = "badInput";
        String ipLow = "badInput2";

        Map<String, Object> config = createConfig("field", field,
                "ipHigh", ipHigh, "ipLow", ipLow );
        IpCompareCondition ipComparatorCondition = new IpCompareCondition.Factory().create(config, conditionParser);
        assertThatThrownBy(() -> ipComparatorCondition.evaluate(createDoc("field1", "192.50.3.0")))
        .isInstanceOf(UnknownHostException.class)
        .hasMessageContaining("nodename nor servname provided");
    }
    
    public void testIncorrectFieldInput() {
        String field = "field1";
        String ipHigh = "192.200.3.0";
        String ipLow = "192.100.3.0";

        Map<String, Object> config = createConfig("field", field,
                "ipHigh", ipHigh, "ipLow", ipLow );
        IpCompareCondition ipComparatorCondition = new IpCompareCondition.Factory().create(config, conditionParser);
        assertThatThrownBy(() -> ipComparatorCondition.evaluate(createDoc("field1", "badinput")))
        .isInstanceOf(UnknownHostException.class)
        .hasMessageContaining("nodename nor servname provided");
    }
    
    public void testBadConfig() {
    	 String field = "field1";
         String ipHigh = "192.200.3.0";
         String ipLow = "192.100.3.0";
         
    	 Map<String, Object> config = createConfig("field", field,"ipHigh", ipHigh);
         IpCompareCondition ipComparatorCondition = new IpCompareCondition.Factory().create(config, conditionParser);
         assertThatThrownBy(() -> ipComparatorCondition.evaluate(createDoc("field1", "192.50.3.0")))
         .isInstanceOf(NullPointerException.class);
         
         config = createConfig("field", field,"ipLow", ipLow);
         IpCompareCondition ipComparatorCondition2 = new IpCompareCondition.Factory().create(config, conditionParser);
         assertThatThrownBy(() -> ipComparatorCondition2.evaluate(createDoc("field1", "192.50.3.0")))
         .isInstanceOf(NullPointerException.class);
         
         config = createConfig("ipLow", ipLow);
         IpCompareCondition ipComparatorCondition3 = new IpCompareCondition.Factory().create(config, conditionParser);
         assertThatThrownBy(() -> ipComparatorCondition3.evaluate(createDoc("fieldWrong", "192.50.3.0")))
         .isInstanceOf(NullPointerException.class);
    	
    }

}
