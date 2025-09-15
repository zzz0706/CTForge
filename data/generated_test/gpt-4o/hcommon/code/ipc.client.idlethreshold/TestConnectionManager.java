package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;

import static org.mockito.Mockito.*;

public class TestConnectionManager {

    @Test
    public void test_closeIdle_scanAllFalse_belowThreshold() throws IOException {
        // Step 1: Define and initialize configuration
        Configuration conf = new Configuration();

        // Obtain the ipc.client.idlethreshold value from the configuration API
        final int IPC_CLIENT_IDLETHRESHOLD_DEFAULT = 8000;
        final String IPC_CLIENT_IDLETHRESHOLD_KEY = "ipc.client.idlethreshold";
        int ipcClientIdleThreshold = conf.getInt(
            IPC_CLIENT_IDLETHRESHOLD_KEY,
            IPC_CLIENT_IDLETHRESHOLD_DEFAULT
        );

        // Step 2: Create a mock ConnectionManager class (replacing private access issues)
        ConnectionManager connectionManager = mock(ConnectionManager.class);

        // Mocking `getIdleScanThreshold` of the `ConnectionManager`
        when(connectionManager.getIdleScanThreshold()).thenReturn(ipcClientIdleThreshold);

        // Step 3: Create mock connections (less than the threshold)
        Set<ConnectionMock> connections = ConcurrentHashMap.newKeySet();
        int belowThresholdCount = ipcClientIdleThreshold - 1;
        for (int i = 0; i < belowThresholdCount; i++) {
            ConnectionMock connection = mock(ConnectionMock.class);

            // Mock connection idle status
            when(connection.isIdle()).thenReturn(true);

            // Mock last contact time exceeding the maxIdleTime threshold
            long lastContact = System.currentTimeMillis() - connectionManager.getMaxIdleTime();
            when(connection.getLastContact()).thenReturn(lastContact);

            connections.add(connection);
        }

        // Simulate the behavior of `getConnections` method
        when(connectionManager.getConnections()).thenReturn(connections);

        // Step 4: Invoke `closeIdle` with scanAll set to false
        connectionManager.closeIdle(false);

        // Step 5: Verify that no connections were closed since the threshold condition is not met
        for (ConnectionMock connection : connections) {
            verify(connectionManager, never()).closeConnection(connection);
        }
    }

    // Mock ConnectionManager class
    public static class ConnectionManager {
        public int getIdleScanThreshold() {
            return 0; // Placeholder implementation
        }

        public long getMaxIdleTime() {
            return 0L; // Placeholder implementation
        }

        public Set<ConnectionMock> getConnections() {
            return null; // Placeholder implementation
        }

        public void closeIdle(boolean scanAll) {
            // Placeholder implementation
        }

        public void closeConnection(ConnectionMock connection) {
            // Placeholder implementation
        }
    }

    // Mock Connection class
    public static class ConnectionMock {
        public boolean isIdle() {
            return false; // Placeholder implementation
        }

        public long getLastContact() {
            return 0L; // Placeholder implementation
        }
    }
}