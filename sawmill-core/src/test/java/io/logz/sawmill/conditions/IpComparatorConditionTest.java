package io.logz.sawmill.conditions;

import io.logz.sawmill.Doc;
import io.logz.sawmill.parser.ConditionParser;
import io.logz.sawmill.utils.FactoryUtils;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createCondition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    public void testIncorrectIpHighOrLowInput() {
        String field = "field1";
        String ipHigh = "badInput";
        String ipLow = "badInput2";

        Map<String, Object> config = createConfig("field", field,
                "ipHigh", ipHigh, "ipLow", ipLow );
        IpCompareCondition ipComparatorCondition = new IpCompareCondition.Factory().create(config, conditionParser);
        assertThat(ipComparatorCondition.evaluate(createDoc("field1", "192.50.3.0"))).isFalse();
    }
    
    @Test
    public void testIncorrectFieldInput() {
        String field = "field1";
        String ipHigh = "192.200.3.0";
        String ipLow = "192.100.3.0";

        Map<String, Object> config = createConfig("field", field,
                "ipHigh", ipHigh, "ipLow", ipLow );
        IpCompareCondition ipComparatorCondition = new IpCompareCondition.Factory().create(config, conditionParser);
        assertThat(ipComparatorCondition.evaluate(createDoc("field1", "badinput"))).isFalse();
    }
    
    @Test
    public void testBadConfigs() {
        assertThatThrownBy(() -> createCondition(IpCompareCondition.class)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> createCondition(IpCompareCondition.class, "ipHigh", "192.200.3.0")).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> createCondition(IpCompareCondition.class, "ipLow", "192.100.3.0")).isInstanceOf(NullPointerException.class);
    }

}
