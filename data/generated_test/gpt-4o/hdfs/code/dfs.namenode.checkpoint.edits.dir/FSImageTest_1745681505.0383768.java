package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;

import java.net.URI;
import java.util.List;

public class FSImageTest {

    @Test
    public void test_getCheckpointEditsDirs_withConfiguredDirectories() {
        // 1. 使用API获取配置值，不要硬编码配置值
        Configuration conf = new Configuration();
        
        // Add the config for checkpointEditsDir for the test
        conf.set("dfs.namenode.checkpoint.edits.dir", "file:///tmp/checkpointEditsDir");

        String defaultName = null;

        // 2. 准备测试条件: 通过有效输入调用方法
        List<URI> checkpointEditsDirs = FSImage.getCheckpointEditsDirs(conf, defaultName);

        // 3. 测试代码: 验证返回的列表是否有效
        assertNotNull("The list of checkpoint edits directories should not be null", checkpointEditsDirs);
        assertFalse("The list of checkpoint edits directories should not be empty", checkpointEditsDirs.isEmpty());

        // 4. 测试后的代码: 遍历和验证列表中的每个URI是否有效
        for (URI uri : checkpointEditsDirs) {
            assertNotNull("Each URI in the checkpoint edits directories list should be valid and not null", uri);
        }
    }
}