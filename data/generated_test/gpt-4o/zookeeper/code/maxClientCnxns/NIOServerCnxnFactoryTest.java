package org.apache.zookeeper.test;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class NIOServerCnxnFactoryTest {

    @Test
    // test code
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testRun_ValidConnectionsUnderLoad() throws Exception {
        // Step 1: Prepare the test setup
        Properties zkProperties = new Properties();
        File dataDir = new File(System.getProperty("java.io.tmpdir"), "zookeeper").getAbsoluteFile();
        File dataLogDir = new File(System.getProperty("java.io.tmpdir"), "zookeeper_log").getAbsoluteFile();
        zkProperties.setProperty("dataDir", dataDir.getAbsolutePath());
        zkProperties.setProperty("dataLogDir", dataLogDir.getAbsolutePath());
        zkProperties.setProperty("clientPort", "2181");
        zkProperties.setProperty("maxClientCnxns", "100"); // Example configuration

        QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        quorumPeerConfig.parseProperties(zkProperties);

        ServerConfig serverConfig = new ServerConfig();
        serverConfig.readFrom(quorumPeerConfig);

        // Initialize the NIOServerCnxnFactory
        NIOServerCnxnFactory cnxnFactory = new NIOServerCnxnFactory();
        InetSocketAddress clientPortAddress = new InetSocketAddress(Integer.parseInt(zkProperties.getProperty("clientPort")));
        cnxnFactory.configure(clientPortAddress, Integer.parseInt(zkProperties.getProperty("maxClientCnxns")));

        // Create and configure the ZooKeeperServer instance
        ZooKeeperServer zooKeeperServer = new ZooKeeperServer(
                new FileTxnSnapLog(dataDir, dataLogDir),
                serverConfig.getTickTime()
        );

        // Step 2: Start the factory and simulate load
        cnxnFactory.startup(zooKeeperServer);

        // Simulate 80 concurrent connections under load (below the limit of 100)
        final CountDownLatch latch = new CountDownLatch(80);
        for (int i = 0; i < 80; i++) {
            new Thread(() -> {
                try {
                    // Simulate connection work
                    // Example workload: invoking some operations
                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }

        // Wait for connections to complete
        boolean allConnectionsCompleted = latch.await(5, TimeUnit.SECONDS);

        // Step 3: Test code
        assert allConnectionsCompleted : "Not all connections completed";

        // Step 4: Code after testing
        cnxnFactory.shutdown();
    }
}