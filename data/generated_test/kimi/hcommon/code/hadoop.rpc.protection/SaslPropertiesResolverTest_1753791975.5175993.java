package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.junit.Test;

import javax.security.sasl.Sasl;
import java.net.InetAddress;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SaslPropertiesResolverTest {

    @Test
    public void testCaseInsensitiveParsing() throws Exception {
        // 1. Prepare the test conditions.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION, "AuThEnTiCaTiOn");

        // 2. Instantiate SaslPropertiesResolver and invoke setConf(conf).
        SaslPropertiesResolver resolver = new SaslPropertiesResolver();
        resolver.setConf(conf);

        // 3. Call getClientProperties(InetAddress.getLocalHost()) and inspect the map.
        Map<String, String> props = resolver.getClientProperties(InetAddress.getLocalHost());

        // 4. Code after testing.
        assertEquals("auth", props.get(Sasl.QOP));
    }
}