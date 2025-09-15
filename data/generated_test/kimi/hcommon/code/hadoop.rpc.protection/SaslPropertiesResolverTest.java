package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;

import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SaslPropertiesResolverTest {

    @Test
    public void testDefaultAuthenticationOnly() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        SaslPropertiesResolver resolver = new SaslPropertiesResolver();
        resolver.setConf(conf);

        // 3. Test code.
        String[] qop = conf.getTrimmedStrings(
            CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION,
            QualityOfProtection.AUTHENTICATION.toString());
        String expectedQop = QualityOfProtection.valueOf(
            StringUtils.toUpperCase(qop[0])).getSaslQop();

        Map<String, String> props = resolver.getDefaultProperties();
        String actualQop = props.get("javax.security.sasl.qop");

        // 4. Code after testing.
        assertEquals(expectedQop, actualQop);
    }
}