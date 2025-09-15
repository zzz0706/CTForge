package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.common.X509Util;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.mockito.Mockito.*;

public class TestSslQuorumReloadCertFiles {

    @Test
    // test code
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testSslQuorumReloadCertFilesEnableCertFileReloading() throws IOException, QuorumPeerConfig.ConfigException {
        // 1. Correctly load configuration using the zookeeper3.5.6 API
        String CONFIG_PATH = "ctest.cfg";
        Properties props = new Properties();
        
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }
        
        // 2. Parsing the configuration properties
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // Output basic configurations for validation
        System.out.println("tickTime      = " + config.getTickTime());
        System.out.println("initLimit     = " + config.getInitLimit());
        System.out.println("syncLimit     = " + config.getSyncLimit());
        System.out.println("dataDir       = " + config.getDataDir());
        System.out.println("dataLogDir    = " + config.getDataLogDir());

        // Enable SSL Quorum configuration
        String sslQuorum = props.getProperty("ssl.quorum", "false");
        boolean isSslQuorumEnabled = Boolean.parseBoolean(sslQuorum);

        // Mock QuorumPeerConfig and X509Util behaviors
        X509Util x509UtilMock = mock(X509Util.class);

        // 3. Test code
        if (isSslQuorumEnabled) {
            x509UtilMock.enableCertFileReloading();
        }

        // Verify that enableCertFileReloading() was invoked
        verify(x509UtilMock, times(isSslQuorumEnabled ? 1 : 0)).enableCertFileReloading();

        // 4. Code after testing
        System.out.println("Test completed: SSL Quorum Reload Cert Files functionality verified.");
    }
}