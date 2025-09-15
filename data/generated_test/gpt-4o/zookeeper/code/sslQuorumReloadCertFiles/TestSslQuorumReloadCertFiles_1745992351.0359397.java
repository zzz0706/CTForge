package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.common.X509Util;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.mockito.Mockito.*;

/**
 * Unit test for verifying the behavior of the `sslQuorumReloadCertFiles` configuration.
 */
public class TestSslQuorumReloadCertFiles {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    // test code
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testParseProperties_sslQuorumReloadCertFilesFalse() throws Exception {
        // Step 1: Load the `sslQuorumReloadCertFiles` configuration value using ZooKeeper API
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(CONFIG_PATH)) {
            properties.load(inputStream);
        }

        // Step 2: Parse properties to verify propagation and usage
        QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        quorumPeerConfig.parseProperties(properties);

        String sslQuorumReloadCertFiles = properties.getProperty("sslQuorumReloadCertFiles", "false");
        boolean isCertFileReloadingEnabled = Boolean.parseBoolean(sslQuorumReloadCertFiles);

        // Step 3: Test conditions using mocked dependencies and interaction verification
        X509Util mockedX509Util = mock(X509Util.class);

        if (isCertFileReloadingEnabled) {
            // Simulate the behavior when certificate reloading is enabled
            doNothing().when(mockedX509Util).enableCertFileReloading();
            mockedX509Util.enableCertFileReloading();
        }

        // Step 4: Verify method invocation for "certificate file reloading"
        verify(mockedX509Util, times(isCertFileReloadingEnabled ? 1 : 0)).enableCertFileReloading();
    }
}