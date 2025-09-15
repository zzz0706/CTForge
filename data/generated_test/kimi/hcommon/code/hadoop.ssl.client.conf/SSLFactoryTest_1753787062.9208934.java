package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class SSLFactoryTest {

    @Test
    public void testNonExistentClientSSLConfigResourceDoesNotFailStartup() throws Exception {
        // 1. Configuration as input
        Configuration conf = new Configuration(false);
        conf.set("hadoop.ssl.client.conf", "nonexistent-ssl-client.xml");

        // 2. Prepare the test conditions.
        // No additional mocking is required; SSLFactory will gracefully ignore
        // the missing resource and fall back to default values.

        // 3. Test code.
        SSLFactory factory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);

        // 4. Code after testing.
        assertNotNull("SSLFactory should initialize successfully", factory);
    }
}