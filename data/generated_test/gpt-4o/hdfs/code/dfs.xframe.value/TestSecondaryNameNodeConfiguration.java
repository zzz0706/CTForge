package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode;
import org.junit.Test;
import java.io.IOException;

public class TestSecondaryNameNodeConfiguration {
    // Test code
    // 1. 使用API获取配置值，不要硬编码配置值
    // 2. 准备测试条件
    // 3. 测试代码
    // 4. 测试后的代码
  
    @Test
    public void test_secondaryNameNode_configuration_functionality() throws Exception {
        // Create a Configuration object
        Configuration configuration = new Configuration();

        // Set necessary configuration values
        // Ensure fs.defaultFS is configured properly to avoid the `Invalid URI` error
        configuration.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "hdfs://localhost:8020");
        configuration.setBoolean(DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED, true);
        configuration.set(DFSConfigKeys.DFS_XFRAME_OPTION_VALUE, DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);
        configuration.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, "127.0.0.1:50090");

        // Instantiate SecondaryNameNode with the correct configuration
        SecondaryNameNode secondaryNameNode = null;
        try {
            secondaryNameNode = new SecondaryNameNode(configuration);

            // Call the `startInfoServer()` method for testing
            secondaryNameNode.startInfoServer();
        } finally {
            // Ensure clean shutdown of SecondaryNameNode to avoid resource leakage
            if (secondaryNameNode != null) {
                secondaryNameNode.shutdown();
            }
        }
    }
}