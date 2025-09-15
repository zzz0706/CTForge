package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class TestSSLFactoryConfiguration {

    @Test
    public void testKeystoresFactoryClassConfiguration() {
        // Step 1: Create a Configuration object to read the configuration values.
        Configuration conf = new Configuration();

        // Step 2: Read the configuration value for "hadoop.ssl.keystores.factory.class".
        String keystoresFactoryClass = conf.get(SSLFactory.KEYSTORES_FACTORY_CLASS_KEY, 
                                                "org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory");

        // Step 3: Validate the configuration value against expected constraints.
        // Constraint: The class should be a valid implementation of KeyStoresFactory.
        try {
            // Try to load the class.
            Class<?> clazz = Class.forName(keystoresFactoryClass);

            // Check if the class implements the KeyStoresFactory interface.
            if (!KeyStoresFactory.class.isAssignableFrom(clazz)) {
                Assert.fail("The configured keystores factory class '" + keystoresFactoryClass + 
                            "' does not implement the KeyStoresFactory interface.");
            }
        } catch (ClassNotFoundException e) {
            // Fail the test if the class cannot be loaded.
            Assert.fail("The configured class '" + keystoresFactoryClass + "' could not be found.");
        }
    }
}