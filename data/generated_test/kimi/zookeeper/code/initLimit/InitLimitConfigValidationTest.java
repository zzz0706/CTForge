package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Properties;

public class InitLimitConfigValidationTest {

    private QuorumPeerConfig config;

    @Before
    public void setUp() {
        config = new QuorumPeerConfig();
    }

    @After
    public void tearDown() {
        config = null;
    }

    @Test
    public void testInitLimitNotSetInDistributedMode() throws IOException, ConfigException {
        // 1. Prepare configuration without initLimit in distributed mode
        Properties props = new Properties();
        props.setProperty("server.1", "localhost:2888:3888");
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");

        // 2. Parse configuration
        config.parseProperties(props);

        // 3. Validate - should NOT throw ConfigException because initLimit has a default value
        config.checkValidity();
    }

    @Test
    public void testInitLimitZeroInDistributedMode() throws IOException, ConfigException {
        // 1. Prepare configuration with initLimit=0 in distributed mode
        Properties props = new Properties();
        props.setProperty("server.1", "localhost:2888:3888");
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        props.setProperty("initLimit", "0");

        // 2. Parse configuration
        config.parseProperties(props);

        // 3. Validate - should NOT throw IllegalArgumentException because initLimit=0 is allowed
        config.checkValidity();
    }

    @Test
    public void testInitLimitPositiveInDistributedMode() throws IOException, ConfigException {
        // 1. Prepare valid configuration with initLimit>0 in distributed mode
        Properties props = new Properties();
        props.setProperty("server.1", "localhost:2888:3888");
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");
        props.setProperty("initLimit", "10");

        // 2. Parse configuration
        config.parseProperties(props);

        // 3. Validate - should pass
        config.checkValidity();

        // 4. Verify initLimit value is correctly parsed
        assertEquals(10, config.getInitLimit());
    }

    @Test
    public void testInitLimitNotRequiredInStandaloneMode() throws IOException, ConfigException {
        // 1. Prepare configuration without initLimit in standalone mode
        Properties props = new Properties();
        props.setProperty("dataDir", "/tmp/zookeeper");
        props.setProperty("clientPort", "2181");

        // 2. Parse configuration
        config.parseProperties(props);

        // 3. Validate - should pass as initLimit is not required in standalone mode
        config.checkValidity();
    }
}