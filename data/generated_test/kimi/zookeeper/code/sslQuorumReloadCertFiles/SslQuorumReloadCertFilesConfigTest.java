package org.apache.zookeeper.server.quorum;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

public class SslQuorumReloadCertFilesConfigTest {

    private QuorumPeerConfig config;
    private Properties props;

    @Before
    public void setUp() {
        props = new Properties();
        // 1. Mandatory properties for QuorumPeerConfig
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        props.setProperty("server.1", "localhost:2888:3888");

        config = new QuorumPeerConfig();
    }

    @After
    public void tearDown() {
        props.clear();
        config = null;
    }

    @Test
    public void testValidTrueValue() throws Exception {
        props.setProperty("sslQuorumReloadCertFiles", "true");
        props.setProperty("sslQuorum", "true");
        props.setProperty("ssl.keystore.location", "/tmp/keystore.jks");
        props.setProperty("ssl.truststore.location", "/tmp/truststore.jks");
        config.parseProperties(props);
        assertTrue(config.sslQuorumReloadCertFiles);
    }

    @Test
    public void testValidFalseValue() throws Exception {
        props.setProperty("sslQuorumReloadCertFiles", "false");
        props.setProperty("sslQuorum", "true");
        props.setProperty("ssl.keystore.location", "/tmp/keystore.jks");
        props.setProperty("ssl.truststore.location", "/tmp/truststore.jks");
        config.parseProperties(props);
        assertFalse(config.sslQuorumReloadCertFiles);
    }

    @Test
    public void testInvalidNonBooleanValue() throws Exception {
        props.setProperty("sslQuorumReloadCertFiles", "invalid");
        props.setProperty("sslQuorum", "true");
        props.setProperty("ssl.keystore.location", "/tmp/keystore.jks");
        props.setProperty("ssl.truststore.location", "/tmp/truststore.jks");
        config.parseProperties(props);
        assertFalse(config.sslQuorumReloadCertFiles);
    }

    @Test
    public void testMissingValue() throws Exception {
        props.setProperty("sslQuorum", "true");
        props.setProperty("ssl.keystore.location", "/tmp/keystore.jks");
        props.setProperty("ssl.truststore.location", "/tmp/truststore.jks");
        config.parseProperties(props);
        assertFalse(config.sslQuorumReloadCertFiles);
    }

    @Test
    public void testDependencySslQuorumFalse() throws Exception {
        props.setProperty("sslQuorumReloadCertFiles", "true");
        props.setProperty("sslQuorum", "false");
        config.parseProperties(props);
        assertTrue(config.sslQuorumReloadCertFiles);
    }

    @Test
    public void testCaseInsensitiveTrue() throws Exception {
        props.setProperty("sslQuorumReloadCertFiles", "TRUE");
        props.setProperty("sslQuorum", "true");
        props.setProperty("ssl.keystore.location", "/tmp/keystore.jks");
        props.setProperty("ssl.truststore.location", "/tmp/truststore.jks");
        config.parseProperties(props);
        assertTrue(config.sslQuorumReloadCertFiles);
    }

    @Test
    public void testCaseInsensitiveFalse() throws Exception {
        props.setProperty("sslQuorumReloadCertFiles", "FALSE");
        props.setProperty("sslQuorum", "true");
        props.setProperty("ssl.keystore.location", "/tmp/keystore.jks");
        props.setProperty("ssl.truststore.location", "/tmp/truststore.jks");
        config.parseProperties(props);
        assertFalse(config.sslQuorumReloadCertFiles);
    }

    @Test
    public void testEmptyValue() throws Exception {
        props.setProperty("sslQuorumReloadCertFiles", "");
        props.setProperty("sslQuorum", "true");
        props.setProperty("ssl.keystore.location", "/tmp/keystore.jks");
        props.setProperty("ssl.truststore.location", "/tmp/truststore.jks");
        config.parseProperties(props);
        assertFalse(config.sslQuorumReloadCertFiles);
    }

    @Test
    public void testWhitespaceValue() throws Exception {
        props.setProperty("sslQuorumReloadCertFiles", "  true  ");
        props.setProperty("sslQuorum", "true");
        props.setProperty("ssl.keystore.location", "/tmp/keystore.jks");
        props.setProperty("ssl.truststore.location", "/tmp/truststore.jks");
        config.parseProperties(props);
        assertTrue(config.sslQuorumReloadCertFiles);
    }
}