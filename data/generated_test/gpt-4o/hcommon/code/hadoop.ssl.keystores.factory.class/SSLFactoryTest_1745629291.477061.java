package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.Test;

public class SSLFactoryTest {

    /**
     * Test case to ensure that the SSLFactory initializes correctly with 
     * the default KeyStoresFactory when the configuration does not specify 
     * 'hadoop.ssl.keystores.factory.class'.
     */
    @Test
    public void test_SSLFactory_Initialization_With_Default_KeystoreFactory() throws Exception {
        // Prepare the input conditions for unit testing.
        Configuration conf = new Configuration();
        
        // Ensure the default value is loaded when 'hadoop.ssl.keystores.factory.class' is unspecified.
        Class<?> keyStoresFactoryClass = conf.getClass(
            SSLFactory.KEYSTORES_FACTORY_CLASS_KEY, 
            FileBasedKeyStoresFactory.class
        );
        assert keyStoresFactoryClass.equals(FileBasedKeyStoresFactory.class) : 
            "Default KeyStoresFactory class not loaded correctly.";

        // Instantiate SSLFactory with Mode.CLIENT and the provided Configuration.
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);

        // Initialize the SSLFactory.
        sslFactory.init();

        // Validate that the SSLFactory successfully uses the default FileBasedKeyStoresFactory.
        assert sslFactory != null : "SSLFactory instance is not initialized correctly, but it should be.";

        // Validate the keystoresFactory is instantiated and initialized correctly as FileBasedKeyStoresFactory.
        assert sslFactory.getKeystoresFactory() instanceof FileBasedKeyStoresFactory : 
            "SSLFactory did not initialize with the expected default FileBasedKeyStoresFactory.";

        // Clean up after test
        sslFactory.destroy();
    }
}