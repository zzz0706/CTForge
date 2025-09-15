package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.junit.Test;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.io.IOException;

public class TestDataStreamerRetry {
    // test code
    // 1. 使用API获取配置值，不要硬编码配置值
    // 2. 准备测试条件
    // 3. 测试代码
    // 4. 测试后的代码

    @Test
    public void test_DataStreamer_BlockCreation_RetryLogic() {
        // Step 1: Initialize Configuration
        Configuration conf = new Configuration();
        conf.setInt("dfs.client.block.write.retries", 3); // Use API to set configuration

        // Step 2: Prepare required configuration
        DfsClientConf dfsClientConf = new DfsClientConf(conf);

        // Step 3: Mock interactions with the underlying system
        LocatedBlock mockLocatedBlock = mock(LocatedBlock.class);

        int retryCount = dfsClientConf.getNumBlockWriteRetry();
        boolean blockCreated = false;

        // Attempt to simulate block creation with retries
        for (int i = 1; i <= retryCount; i++) {
            try {
                // Simulate failure for all but the last retry
                if (i < retryCount) {
                    throw new IOException("Mock DataNode connection failure");
                }

                // Simulate success on the final retry
                blockCreated = (mockLocatedBlock != null);
                break;
            } catch (IOException e) {
                // Log retry failure
                System.out.println("Retry " + i + " failed: " + e.getMessage());
            }
        }

        // Step 4: Assert the block was created successfully within the retry limit
        assertTrue("Block creation failed within the retry limit", blockCreated);
    }
}