package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import static org.junit.Assert.fail;

public class SaslPropertiesResolverTest {

    @Test
    public void testInvalidProtectionValueThrowsIllegalArgumentException() {
        // 1. Create Configuration and set invalid value
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION, "invalid");

        // 2. Instantiate SaslPropertiesResolver and attempt to invoke setConf(conf)
        SaslPropertiesResolver resolver = new SaslPropertiesResolver();
        try {
            resolver.setConf(conf);
            fail("Expected IllegalArgumentException for invalid protection value");
        } catch (IllegalArgumentException e) {
            // 3. Verify exception is thrown with expected message
            String expectedMessage = "No enum constant org.apache.hadoop.security.SaslRpcServer.QualityOfProtection.INVALID";
            if (!e.getMessage().contains(expectedMessage)) {
                throw e;
            }
        }
    }
}