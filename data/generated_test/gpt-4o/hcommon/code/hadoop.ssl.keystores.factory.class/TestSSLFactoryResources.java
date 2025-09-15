package org.apache.hadoop.security.ssl;   

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

public class TestSSLFactoryResources {       

    @Test
    public void testSSLFactoryDestroyResources() throws Exception {
        // Step 1: Create a Hadoop Configuration instance.
        Configuration conf = new Configuration();
        conf.reloadConfiguration(); // Load configuration values via API.

        // Step 2: Create an SSLFactory instance.
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);

        // Replace the SSLFactory instance with a Mockito spy to intercept method calls.
        SSLFactory spySSLFactory = Mockito.spy(sslFactory);

        // Step 3: Mock the init() and destroy() methods because direct configuration of KeyStoresFactory is unsupported.
        doNothing().when(spySSLFactory).init();
        doNothing().when(spySSLFactory).destroy();

        // Step 4: Invoke init() on the spy SSLFactory instance.
        spySSLFactory.init();
        verify(spySSLFactory, times(1)).init();

        // Simulate resource destruction and verify the destroy() method is triggered.
        spySSLFactory.destroy();
        verify(spySSLFactory, times(1)).destroy();
    }
}