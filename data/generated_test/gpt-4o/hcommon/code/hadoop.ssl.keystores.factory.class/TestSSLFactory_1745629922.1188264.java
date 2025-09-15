package org.apache.hadoop.security.ssl;  

import org.apache.hadoop.conf.Configuration;       
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestSSLFactory {      
 

    // Prepare the input conditions for unit testing.
    @Test
    public void test_SSLFactory_ThrowExceptionForNullMode() {
        // Create a Hadoop Configuration instance
        Configuration conf = new Configuration(); 
        try {
            // Attempt to instantiate the SSLFactory while passing 'null' as the Mode parameter
            SSLFactory factory = new SSLFactory(null, conf);
            fail("Expected IllegalArgumentException was not thrown");
        } catch (IllegalArgumentException e) {
            // Ensure the exception contains the message 'mode cannot be NULL'
            assertTrue(e.getMessage().contains("mode cannot be NULL"));
        }
    }
}