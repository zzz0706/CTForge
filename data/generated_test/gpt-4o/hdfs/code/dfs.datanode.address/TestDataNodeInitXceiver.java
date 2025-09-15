package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class TestDataNodeInitXceiver {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_initDataXceiver_ValidConfiguration() {
        Configuration conf = new Configuration();

        // 1. Prepare the test configuration.
        // Use the hdfs 2.8.5 API to fetch the dfs.datanode.address value or use the default.
        String dataNodeAddress = conf.getTrimmed(
                DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY,
                DFSConfigKeys.DFS_DATANODE_ADDRESS_DEFAULT
        );

        try {
            // 2. Prepare storage locations for DataNode initialization.
            List<StorageLocation> storageLocations = new ArrayList<>();
            StorageLocation location = StorageLocation.parse("file:///tmp/data");
            storageLocations.add(location);

            // Initialize SecureResources (simulate null for the test as an example).
            SecureDataNodeStarter.SecureResources secureResources = null;

            // 3. Initialize the DataNode using correct constructor available in HDFS 2.8.5.
            DataNode dataNode = DataNode.instantiateDataNode(
                    new String[]{},
                    conf,
                    secureResources
            );

            if (dataNode != null) {
                dataNode.runDatanodeDaemon();

                // Verify TcpPeerServer binding and streams.
                InetSocketAddress actualAddress = dataNode.getXferAddress();
                InetSocketAddress expectedAddress = NetUtils.createSocketAddr(dataNodeAddress);
                
                System.out.println("Expected address: " + expectedAddress.toString());
                System.out.println("Actual address: " + actualAddress.toString());

                // Assert the address matches the expected setup.
                if (!expectedAddress.equals(actualAddress)) {
                    throw new AssertionError("TcpPeerServer address is incorrect.");
                }

                System.out.println("TcpPeerServer initialized and bound to the correct address.");
            } else {
                throw new AssertionError("DataNode initialization failed.");
            }

        } catch (IOException e) {
            // Handle exceptions if any initialization issues occur.
            e.printStackTrace();
            System.out.println("Failed to initialize DataNode or TcpPeerServer. Ensure valid configuration.");
        } finally {
            // 4. Clean up resources after testing.
            GenericTestUtils.assertNoThreadsMatching("DataXceiverServer");
        }
    }
}