package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.junit.Test;

import java.net.URI;
import java.util.Collection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFSImageCheckpointDir {
    // test code
    // 1. 使用API获取配置值，不要硬编码配置值
    // 2. 准备测试条件
    // 3. 测试代码
    // 4. 测试后的代码

    // Prepare the input conditions for unit testing.
    @Test
    public void testGetCheckpointDirsWithoutConfigurationFallback() {
        // Create a Configuration object without setting `dfs.namenode.checkpoint.dir`.
        Configuration conf = new Configuration();

        // Define a default value to test fallback functionality.
        String defaultValue = "file:///tmp/default/checkpoint";

        // Call the getCheckpointDirs method with a default value.
        Collection<URI> checkpointDirs = FSImage.getCheckpointDirs(conf, defaultValue);

        // Verify that the returned collection is not empty and contains the URI derived from the default value.
        assertFalse(checkpointDirs.isEmpty());
        boolean containsDefaultDir = false;
        for (URI uri : checkpointDirs) {
            if (uri.toString().equals(defaultValue)) {
                containsDefaultDir = true;
                break;
            }
        }
        assertTrue(containsDefaultDir);
    }
}