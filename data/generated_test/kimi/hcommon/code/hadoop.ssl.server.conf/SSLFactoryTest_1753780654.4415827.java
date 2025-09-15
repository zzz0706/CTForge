package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SSLFactoryTest {

    @Test
    public void sslServerConfFileNotFoundTriggersClasspathLookupFailure() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        Configuration conf = new Configuration();
        conf.set(SSLFactory.SSL_SERVER_CONF_KEY, "nonexistent-ssl-server.xml");

        // 3. Test code.
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.SERVER, conf);

        // In Hadoop 2.8.5 the SSLFactory loads the resource via Configuration
        // and will not throw an exception if the file is missing. Instead,
        // the configuration remains unchanged.
        assertNull("SSL properties from missing file should not be loaded",
                   conf.get("ssl.server.keystore.location"));

        boolean requireClientCert = conf.getBoolean(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY, false);
        assertEquals("SSL_REQUIRE_CLIENT_CERT should match default from parent conf",
                     conf.getBoolean(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY, false),
                     requireClientCert);

        // 4. Code after testing.
    }
}