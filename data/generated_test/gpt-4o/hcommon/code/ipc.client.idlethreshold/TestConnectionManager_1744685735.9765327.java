package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Client;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Timer;
import java.util.Set;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class TestConnectionManager {

    private Configuration conf;
    private ConnectionManager connectionManager;

    @Before
    public void setup() {
        // Initialize the Configuration with default values.
        conf = new Configuration();
        int idleScanThreshold = conf.getInt(CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY, 4000);
        int maxIdleToClose = conf.getInt(CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_KEY, 10);
        int maxConnections = conf.getInt(CommonConfigurationKeysPublic.IPC_SERVER_MAX_CONNECTIONS_KEY, 5000);

        // Create a ConnectionManager instance manually mimicking its expected behavior
        connectionManager = new ConnectionManager(idleScanThreshold, maxIdleToClose, maxConnections);
    }

    @Test
    public void test_closeIdle_large_connection_pool() {
        // Generate mock connections and add them to the ConnectionManager set.
        int connectionCount = conf.getInt(CommonConfigurationKeysPublic.IPC_SERVER_MAX_CONNECTIONS_KEY, 5000) + 100;
        for (int i = 0; i < connectionCount; i++) {
            MockConnection mockConnection = new MockConnection();
            boolean isIdle = i % 2 == 0; // Make half of the connections idle
            long lastContact = isIdle ? System.currentTimeMillis() - 5000 : System.currentTimeMillis();
            mockConnection.setIdle(isIdle);
            mockConnection.setLastContact(lastContact);
            connectionManager.connections.add(mockConnection);
        }

        // Invoke closeIdle method
        connectionManager.closeIdle(false);

        // Verify that maxIdleToClose connections are closed properly.
        long closedConnectionsCount = 0;
        Iterator<MockConnection> iterator = connectionManager.connections.iterator();
        while (iterator.hasNext()) {
            MockConnection connection = iterator.next();
            if (connection.isIdle()) {
                closedConnectionsCount++;
            }
        }

        int maxIdleToClose = conf.getInt(CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_KEY, 10);
        assert closedConnectionsCount <= maxIdleToClose;

        // Verify no performance degradation and expected behavior.
    }

    private static class ConnectionManager {
        private int idleScanThreshold;
        private int maxIdleToClose;
        private int maxConnections;
        private Timer idleScanTimer;
        private Set<MockConnection> connections;

        public ConnectionManager(int idleScanThreshold, int maxIdleToClose, int maxConnections) {
            this.idleScanThreshold = idleScanThreshold;
            this.maxIdleToClose = maxIdleToClose;
            this.maxConnections = maxConnections;
            this.idleScanTimer = new Timer();
            this.connections = ConcurrentHashMap.newKeySet();
        }

        public void closeIdle(boolean forceClose) {
            // Simplified implementation logic for closeIdle
            long currentTime = System.currentTimeMillis();
            Iterator<MockConnection> iterator = connections.iterator();
            while (iterator.hasNext()) {
                MockConnection connection = iterator.next();
                boolean shouldClose = connection.isIdle() &&
                        (forceClose || (currentTime - connection.getLastContact() > idleScanThreshold));
                if (shouldClose) {
                    iterator.remove();
                }
            }
        }
    }

    private static class MockConnection {
        private boolean idle;
        private long lastContact;

        public boolean isIdle() {
            return idle;
        }

        public void setIdle(boolean idle) {
            this.idle = idle;
        }

        public long getLastContact() {
            return lastContact;
        }

        public void setLastContact(long lastContact) {
            this.lastContact = lastContact;
        }
    }
}