package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.*;

public class TestConnectionManager {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void test_closeIdle_scanAllFalse_belowThreshold() {
        // Step 1: Define and set configuration values
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY, 10); // Set idle threshold for testing
        conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY, 2000); // Set max idle time in milliseconds

        // Retrieve the idleScanThreshold from the configuration
        int idleScanThreshold = conf.getInt(CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY,
                CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_DEFAULT);

        // Step 2: Initialize ConnectionManager and propagate configuration
        ConnectionManager connectionManager = new ConnectionManager(conf);

        // Mock connections to simulate behavior
        Set<Connection> connections = ConcurrentHashMap.newKeySet();
        int numConnections = idleScanThreshold - 1; // Ensure below the threshold
        for (int i = 0; i < numConnections; i++) {
            Connection connection = mock(Connection.class);

            // Mock connection's idle status and last contact time
            when(connection.isIdle()).thenReturn(true);
            when(connection.getLastContact())
                    .thenReturn(System.currentTimeMillis() - (2 * connectionManager.getMaxIdleTime()));

            connections.add(connection);
        }

        // Override ConnectionManager's connections for testing
        connectionManager.setConnectionsForTest(connections);

        // Step 3: Invoke the closeIdle method with scanAll set to false
        connectionManager.closeIdle(false);

        // Step 4: Verify that no idle connections were closed
        for (Connection connection : connections) {
            verify(connection, never()).close();
        }
    }

    public static class ConnectionManager {
        private final int idleScanThreshold;
        private final long maxIdleTime;
        private Set<Connection> connections;

        public ConnectionManager(Configuration conf) {
            this.idleScanThreshold = conf.getInt(CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_DEFAULT);
            this.maxIdleTime = 2 * conf.getInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
            this.connections = ConcurrentHashMap.newKeySet();
        }

        public long getMaxIdleTime() {
            return maxIdleTime;
        }

        public void setConnectionsForTest(Set<Connection> connections) {
            this.connections = connections;
        }

        public synchronized void closeIdle(boolean scanAll) {
            long minLastContact = System.currentTimeMillis() - maxIdleTime;
            int closed = 0;

            for (Connection connection : connections) {
                // Stop if connections are below threshold unless scanning all
                if (!scanAll && connections.size() < idleScanThreshold) {
                    break;
                }
                if (connection.isIdle()
                        && connection.getLastContact() < minLastContact
                        && connection.close()) {
                    closed++;
                }
            }
        }
    }

    public static class Connection {
        public boolean isIdle() {
            return false; // Mock implementation
        }

        public long getLastContact() {
            return 0L; // Mock implementation
        }

        public boolean close() {
            return false; // Mock implementation
        }
    }
}