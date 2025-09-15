package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import java.io.File;
import java.io.FileWriter;
import java.net.InetSocketAddress;

import static org.junit.Assert.*;

public class ClientPortAddressConfigTest {

    private static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    private File createTempCfg(String content) throws Exception {
        File cfg = File.createTempFile("zoo", ".cfg", new File(TMP_DIR));
        try (FileWriter w = new FileWriter(cfg)) {
            w.write(content);
        }
        cfg.deleteOnExit();
        return cfg;
    }

    @Test
    public void testClientPortAddressValidIPv4() throws Exception {
        File cfg = createTempCfg("clientPort=2181\nclientPortAddress=127.0.0.1\ntickTime=2000\ndataDir=" + TMP_DIR);
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(cfg.getAbsolutePath());
        InetSocketAddress address = config.getClientPortAddress();
        assertNotNull("clientPortAddress should not be null", address);
        assertEquals("/127.0.0.1", address.getAddress().toString());
        assertEquals(2181, address.getPort());
    }

    @Test
    public void testClientPortAddressValidIPv6() throws Exception {
        File cfg = createTempCfg("clientPort=2181\nclientPortAddress=::1\ntickTime=2000\ndataDir=" + TMP_DIR);
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(cfg.getAbsolutePath());
        InetSocketAddress address = config.getClientPortAddress();
        assertNotNull("clientPortAddress should not be null", address);
        assertEquals("/0:0:0:0:0:0:0:1", address.getAddress().toString());
        assertEquals(2181, address.getPort());
    }

    @Test
    public void testClientPortAddressValidHostname() throws Exception {
        File cfg = createTempCfg("clientPort=2181\nclientPortAddress=localhost\ntickTime=2000\ndataDir=" + TMP_DIR);
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(cfg.getAbsolutePath());
        InetSocketAddress address = config.getClientPortAddress();
        assertNotNull("clientPortAddress should not be null", address);
        assertEquals("localhost", address.getHostName());
        assertEquals(2181, address.getPort());
    }

    @Test
    public void testClientPortAddressMissing() throws Exception {
        File cfg = createTempCfg("clientPort=2181\ntickTime=2000\ndataDir=" + TMP_DIR);
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(cfg.getAbsolutePath());
        InetSocketAddress address = config.getClientPortAddress();
        assertNotNull("clientPortAddress should be set from clientPort", address);
        assertEquals(2181, address.getPort());
    }

    @Test(expected = QuorumPeerConfig.ConfigException.class)
    public void testClientPortAddressInvalidPort() throws Exception {
        File cfg = createTempCfg("clientPort=2181\nclientPortAddress=127.0.0.1:99999\ntickTime=2000\ndataDir=" + TMP_DIR);
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(cfg.getAbsolutePath());
    }

    @Test(expected = QuorumPeerConfig.ConfigException.class)
    public void testClientPortAddressInvalidFormat() throws Exception {
        File cfg = createTempCfg("clientPort=2181\nclientPortAddress=invalid:format\ntickTime=2000\ndataDir=" + TMP_DIR);
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(cfg.getAbsolutePath());
    }

    @Test
    public void testStandaloneServerConfigValidAddress() throws Exception {
        File cfg = createTempCfg("clientPort=2181\nclientPortAddress=0.0.0.0\ntickTime=2000\ndataDir=" + TMP_DIR);
        ServerConfig config = new ServerConfig();
        config.parse(cfg.getAbsolutePath());
        InetSocketAddress address = config.getClientPortAddress();
        assertNotNull("clientPortAddress should not be null", address);
        assertEquals("/0.0.0.0", address.getAddress().toString());
        assertEquals(2181, address.getPort());
    }
}