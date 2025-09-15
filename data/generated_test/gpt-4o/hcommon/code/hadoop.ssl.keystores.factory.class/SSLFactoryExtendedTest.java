package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.Test;

import java.io.IOException;
import java.security.GeneralSecurityException;

public class SSLFactoryExtendedTest {

    /**
     * Test case to ensure that the SSLFactory initializes correctly with 
     * the default KeyStoresFactory when the configuration does not specify 
     * 'hadoop.ssl.keystores.factory.class', and validate its behavior during initialization.
     */
    @Test
    public void test_SSLFactory_Initialization_With_Default_KeystoreFactory() throws Exception {
        // Prepare the input conditions for unit testing.
        Configuration conf = new Configuration();
        
        // Validate that the default KeyStoresFactory class is used.
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
        assert sslFactory != null : "SSLFactory instance is not initialized correctly.";
        assert sslFactory.getKeystoresFactory() instanceof FileBasedKeyStoresFactory : 
            "SSLFactory did not initialize with the default FileBasedKeyStoresFactory.";

        // Clean up after test
        sslFactory.destroy();
    }

    /**
     * Test case to directly invoke the init() method and validate configuration propagation
     * and secure initialization.
     */
    @Test
    public void test_SSLFactory_Init_Method_Execution() throws GeneralSecurityException, IOException {
        // Prepare the input conditions for testing.
        Configuration conf = new Configuration();

        // Instantiate SSLFactory with Mode.CLIENT and verify initialization behavior.
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);

        // Ensure init can be executed properly.
        sslFactory.init();

        // Validate that context and keystoresFactory are initialized after init call.
        assert sslFactory.getKeystoresFactory() != null : 
            "KeystoresFactory instance should be initialized but is null.";
        assert sslFactory.getHostnameVerifier() != null : 
            "Hostname verifier should be initialized but is null.";

        // Clean up after test
        sslFactory.destroy();
    }
}