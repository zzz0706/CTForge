package org.apache.zookeeper.server.quorum;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
//ZOOKEEPER-2299
public class LocalPeerBeanConfigTest {
    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    public void testGetClientAddressShouldNotThrowNPE() throws Exception {
        // Step 1: Load properties from the external config file
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // Step 2: Parse properties using QuorumPeerConfig
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // Step 3: Create a QuorumPeer instance with config, set cnxnFactory to null
        QuorumPeer peer = new QuorumPeer();
        peer.cnxnFactory = null;
        try {
            java.lang.reflect.Field clientPortField = QuorumPeer.class.getDeclaredField("clientPort");
            clientPortField.setAccessible(true);
            clientPortField.setInt(peer, config.getClientPortAddress() != null ? config.getClientPortAddress().getPort() : -1);
        } catch (Exception e) {
            // Ignore reflection errors
        }

        // Step 4: Construct LocalPeerBean
        LocalPeerBean localPeerBean = new LocalPeerBean(peer);

        // Step 5: Check if getClientAddress throws NPE
        boolean npeThrown = false;
        try {
            String addr = localPeerBean.getClientAddress();
            assertNotNull(addr);
        } catch (NullPointerException npe) {
            npeThrown = true;
        }
        assertFalse("NullPointerException thrown:", npeThrown);
    }
}
