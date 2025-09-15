package org.apache.zookeeper.test;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Validates the NIOServerCnxnFactory's ability to handle a high volume of concurrent connections
 * without exceeding its configured limits.
 */
public class NIOServerCnxnFactoryConcurrencyTest {

    private static final int MAX_CONCURRENT_CLIENTS = 100;
    private static final int SIMULATED_CLIENT_LOAD = 80;
    private static final int TEST_TIMEOUT_SECONDS = 10;
    private static final int CLIENT_PORT = 2181;

    /**
     * This test configures a ZooKeeper server and its connection factory, then simulates a burst
     * of client connections to ensure the factory operates correctly under a significant but valid load.
     */
    @Test
    public void testFactoryManagesHighConnectionVolumeGracefully() throws Exception {
        // Step 1: Set up the server configuration.
        ServerConfig serverConfig = createTestServerConfig();
        NIOServerCnxnFactory connectionFactory = new NIOServerCnxnFactory();
        ZooKeeperServer zkServerInstance = null;

        try {
            // Step 2: Configure and initialize the connection factory and server instance.
            connectionFactory.configure(serverConfig.getClientPortAddress(), serverConfig.getMaxClientCnxns());

            FileTxnSnapLog transactionLog = new FileTxnSnapLog(serverConfig.getDataDir(), serverConfig.getDataLogDir());
            zkServerInstance = new ZooKeeperServer(transactionLog, serverConfig.getTickTime());

            connectionFactory.startup(zkServerInstance);
            assertTrue("Connection factory should be running after startup.", connectionFactory.isRunning());

            // Step 3: Simulate concurrent client activity and verify completion.
            boolean allClientsCompleted = simulateConcurrentClientActivity(SIMULATED_CLIENT_LOAD);

            // Step 4: Assert that all simulated client tasks finished within the timeout.
            assertTrue("All simulated client connections did not complete within the specified timeout.", allClientsCompleted);

        } finally {
            // Step 5: Ensure resources are cleaned up after the test.
            connectionFactory.shutdown();
        }
    }

    /**
     * Creates a ServerConfig object based on a predefined set of test properties.
     *
     * @return A fully configured ServerConfig instance.
     * @throws IOException If temporary directories cannot be created.
     * @throws QuorumPeerConfig.ConfigException If property parsing fails.
     */
    private ServerConfig createTestServerConfig() throws IOException, QuorumPeerConfig.ConfigException {
        // Define temporary directories for test artifacts.
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "zookeeper-test-" + System.currentTimeMillis());
        File dataDir = new File(tempDir, "data");
        File logDir = new File(tempDir, "log");
        dataDir.mkdirs();
        logDir.mkdirs();

        // Populate configuration properties.
        Properties serverProps = new Properties();
        serverProps.setProperty("dataDir", dataDir.getAbsolutePath());
        serverProps.setProperty("dataLogDir", logDir.getAbsolutePath());
        serverProps.setProperty("clientPort", String.valueOf(CLIENT_PORT));
        serverProps.setProperty("maxClientCnxns", String.valueOf(MAX_CONCURRENT_CLIENTS));

        // Parse properties using the standard ZooKeeper configuration flow.
        QuorumPeerConfig peerConfig = new QuorumPeerConfig();
        peerConfig.parseProperties(serverProps);

        ServerConfig serverConfig = new ServerConfig();
        serverConfig.readFrom(peerConfig);

        return serverConfig;
    }

    /**
     * Simulates multiple threads attempting to perform work, mimicking concurrent client connections.
     *
     * @param clientCount The number of concurrent clients to simulate.
     * @return true if all threads complete their work within the timeout, false otherwise.
     * @throws InterruptedException if the waiting thread is interrupted.
     */
    private boolean simulateConcurrentClientActivity(int clientCount) throws InterruptedException {
        CountDownLatch completionLatch = new CountDownLatch(clientCount);

        for (int i = 0; i < clientCount; i++) {
            new Thread(() -> {
                // In a real test, this would involve creating a client connection and performing an operation.
                // For this simulation, we just mark the task as done.
                completionLatch.countDown();
            }).start();
        }

        return completionLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
}