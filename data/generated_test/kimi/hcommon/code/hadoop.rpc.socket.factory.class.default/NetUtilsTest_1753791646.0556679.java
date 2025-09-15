package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import static org.junit.Assert.*;

public class NetUtilsTest {

    @Test
    public void testNonexistentClassThrowsRuntimeException() {
        // 1. Create a Configuration instance.
        Configuration conf = new Configuration();

        // 2. Set the configuration to a non-existent class name.
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
                 "com.example.DoesNotExist");

        // 3. Attempt to call NetUtils.getDefaultSocketFactory(conf).
        try {
            NetUtils.getDefaultSocketFactory(conf);
            fail("Expected RuntimeException was not thrown");
        } catch (RuntimeException e) {
            // 4. Verify the exception message contains the expected text.
            assertTrue("Exception message should contain 'Socket Factory class not found'",
                       e.getMessage().contains("Socket Factory class not found"));
        }
    }
}