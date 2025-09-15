package org.apache.hadoop.net;   

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.StandardSocketFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestHadoopRpcSocketFactory {       
    /**
     * Test that validates whether the configuration value for hadoop.rpc.socket.factory.class.default 
     * satisfies constraints and dependencies.
     */
    @Test
    public void testHadoopRpcSocketFactoryClassDefaultConfig() {
        // Initialize a Hadoop configuration
        Configuration config = new Configuration();

        // Step 1: Retrieve the configuration value
        String configValue = config.get(
                CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
                CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_DEFAULT);

        // Step 2: Check if the configuration value satisfies constraints and dependencies
        // The constraints:
        // 1. If the configuration is empty, it defaults to StandardSocketFactory
        // 2. The configuration value must refer to a valid class name that can be instantiated
        if ((configValue == null) || (configValue.isEmpty())) {
            // Check that the default value is used
            try {
                Class<?> defaultClass = Class.forName(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_DEFAULT);
                assertTrue(StandardSocketFactory.class.isAssignableFrom(defaultClass));
            } catch (ClassNotFoundException e) {
                fail("Default Socket Factory class not found: " + CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_DEFAULT);
            }
        } else {
            try {
                // Dynamically load the class specified in the configuration value
                Class<?> socketFactoryClass = Class.forName(configValue);

                // Validate that the class can be instantiated as StandardSocketFactory
                assertTrue(StandardSocketFactory.class.isAssignableFrom(socketFactoryClass));

                // Validate that the instance can be created successfully
                StandardSocketFactory factoryInstance = (StandardSocketFactory) ReflectionUtils.newInstance(socketFactoryClass, config);
                assertNotNull(factoryInstance);
            } catch (ClassNotFoundException e) {
                // Fail the test if the class cannot be found
                fail("Socket Factory class not found: " + configValue);
            } catch (RuntimeException e) {
                // Fail the test if any unexpected exception is thrown during validation
                fail("Unexpected exception: " + e.getMessage());
            }
        }
    }
}