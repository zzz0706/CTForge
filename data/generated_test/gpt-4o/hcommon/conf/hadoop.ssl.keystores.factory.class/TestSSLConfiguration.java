package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.KeyStoresFactory;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestSSLConfiguration {

    /**
     * Test to validate the configuration value for hadoop.ssl.keystores.factory.class
     * and ensure it meets the constraints and dependencies.
     */
    @Test
    public void testKeystoresFactoryClassConfiguration() {
        // Step 1: Load configuration and retrieve the value of the property
        Configuration conf = new Configuration();
        String keystoresFactoryClass = conf.get(
                SSLFactory.KEYSTORES_FACTORY_CLASS_KEY,
                FileBasedKeyStoresFactory.class.getName()
        );

        // Step 2: Validate that the configuration value meets constraints and dependencies
        try {
            Class<?> clazz = Class.forName(keystoresFactoryClass);
            // Check if the class implements KeyStoresFactory interface
            assertTrue("The keystores factory class must implement KeyStoresFactory.",
                    KeyStoresFactory.class.isAssignableFrom(clazz));

            // Check if the class can be instantiated without errors
            KeyStoresFactory instance = (KeyStoresFactory) ReflectionUtils.newInstance(clazz, conf);
            assertNotNull("The instantiated KeyStoresFactory object should not be null.", instance);

        } catch (ClassNotFoundException e) {
            fail("The specified keystores factory class does not exist: " + keystoresFactoryClass);
        } catch (Exception e) {
            fail("Failed to instantiate the specified keystores factory class: " + keystoresFactoryClass);
        }
    }
}