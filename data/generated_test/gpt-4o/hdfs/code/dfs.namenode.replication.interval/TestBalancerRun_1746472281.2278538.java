package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.balancer.Balancer;
import org.apache.hadoop.hdfs.server.balancer.BalancerParameters;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import java.net.URI;
import java.util.Collections;
import java.util.Collection;

public class TestBalancerRun {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testBalancerRun_WithEmptyNameNodeList() throws Exception {
        // 1. Use the hdfs 2.8.5 API to obtain configuration values
        Configuration conf = new Configuration();
        // Retrieve heartbeat interval and replication interval from the configuration
        long heartbeatInterval = conf.getLong(
                DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
                DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT
        );
        long replicationInterval = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT
        );

        // Calculate sleep time based on configuration values
        long sleeptime = heartbeatInterval * 2000 + replicationInterval * 1000;

        // 2. Prepare the test conditions
        // Create an empty NameNode list
        Collection<URI> namenodes = Collections.emptyList();

        // Use default BalancerParameters
        BalancerParameters parameters = BalancerParameters.DEFAULT;

        // 3. Test code
        // Run the Balancer with the given parameters and configuration
        int exitCode = Balancer.run(namenodes, parameters, conf);

        // 4. Validate results to ensure the Balancer exits with success status
        assert exitCode == ExitStatus.SUCCESS.getExitCode();
    }
}