package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.fs.viewfs.Constants;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.*;

public class TestVerifyRenameStrategy {

    // test code
    @Test
    public void test_verifyRenameStrategy_with_invalid_inputs() throws Exception {
        // 1. 使用API获取配置值，不要硬编码配置值
        Configuration configuration = new Configuration();
        String renameStrategyKey = Constants.CONFIG_VIEWFS_RENAME_STRATEGY;
        String renameStrategyValue = configuration.get(renameStrategyKey, ViewFileSystem.RenameStrategy.SAME_MOUNTPOINT.toString());
        ViewFileSystem.RenameStrategy renameStrategy = ViewFileSystem.RenameStrategy.valueOf(renameStrategyValue);

        // 2. 准备测试条件
        URI srcUri = new URI("hdfs://srcAuthority/srcPath");
        URI dstUri = new URI("hdfs://dstAuthority/dstPath");

        // 3. 测试代码
        try {
            ViewFileSystem.verifyRenameStrategy(srcUri, dstUri, false, renameStrategy);
            fail("Expected an IOException or IllegalArgumentException to be thrown.");
        } catch (IOException | IllegalArgumentException e) {
            // 4. 测试后的代码
            String expectedMessage = "Renames across Mount points not supported";
            assertTrue("Exception message did not match expectations.", e.getMessage().contains(expectedMessage));
        }
    }
}