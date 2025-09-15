package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestDfsClientSlowIoConfigurations {
    // Test code
    // 1. 使用API获取配置值，不要硬编码配置值
    // 2. 准备测试条件
    // 3. 测试代码
    // 4. 测试后的代码

    @Test
    public void test_getSlowIoWarningThresholdMs_custom_value() {
        // Step 1: Create a Configuration object
        Configuration configuration = new Configuration();

        // Step 2: Set a custom value for the configuration parameter
        long customSlowIoThresholdMs = 5000L;
        configuration.setLong(HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY, customSlowIoThresholdMs);

        // Step 3: Initialize DfsClientConf with the Configuration object
        DfsClientConf dfsClientConf = new DfsClientConf(configuration);

        // Step 4: Call getSlowIoWarningThresholdMs and assert the configured value is returned
        assertEquals("The custom slow I/O warning threshold value was not fetched correctly.",
                customSlowIoThresholdMs,
                dfsClientConf.getSlowIoWarningThresholdMs());
    }
}