package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.util.StringUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Map;

import org.junit.Test;

public class SaslPropertiesResolverTest {

    @Test
    public void testMultipleQopValues() {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        // NOTE: Do NOT call conf.set(...) here – rely on defaults or test-resource overrides

        // 2. Prepare the test conditions.
        // Ensure the property is set externally (e.g., test resource XML) to 'authentication,integrity,privacy'
        // Read the actual value dynamically
        String[] qopValues = conf.getTrimmedStrings(
                CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION,
                "authentication");

        // Compute expected SASL QOP string
        StringBuilder expectedQop = new StringBuilder();
        for (int i = 0; i < qopValues.length; i++) {
            // Convert configuration value to uppercase before matching
            String upper = StringUtils.toUpperCase(qopValues[i]);
            String saslQop;
            switch (upper) {
                case "AUTHENTICATION":
                    saslQop = "auth";
                    break;
                case "INTEGRITY":
                    saslQop = "auth-int";
                    break;
                case "PRIVACY":
                    saslQop = "auth-conf";
                    break;
                default:
                    saslQop = "auth";
            }
            qopValues[i] = saslQop;
            if (i > 0) expectedQop.append(",");
            expectedQop.append(qopValues[i]);
        }

        // 3. Test code.
        SaslPropertiesResolver resolver = new SaslPropertiesResolver();
        resolver.setConf(conf);
        Map<String, String> props = resolver.getDefaultProperties();

        // 4. Code after testing.
        assertNotNull(props);
        assertEquals(expectedQop.toString(), props.get("javax.security.sasl.qop"));
    }
}