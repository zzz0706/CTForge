package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.blockmanagement.HostConfigManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager;
import org.mockito.Mockito;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestDatanodeManagerInitialization {

    @Test
    public void testDatanodeManagerInitializationWithDefaultConfiguration() throws IOException {
        // 1. 使用API获取配置值，不要硬编码配置值
        Configuration configuration = new Configuration();

        // 2. 准备测试条件
        BlockManager mockBlockManager = Mockito.mock(BlockManager.class);
        Namesystem mockNamesystem = Mockito.mock(Namesystem.class);

        // 3. 初始化DatanodeManager
        DatanodeManager datanodeManager = new DatanodeManager(mockBlockManager, mockNamesystem, configuration);

        // 4. 测试代码
        HostConfigManager hostConfigManager = datanodeManager.getHostConfigManager();
        Assert.assertTrue(hostConfigManager instanceof HostFileManager);
    }
}