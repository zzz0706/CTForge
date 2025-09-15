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
     
        Configuration configuration = new Configuration();
        String renameStrategyKey = Constants.CONFIG_VIEWFS_RENAME_STRATEGY;
        String renameStrategyValue = configuration.get(renameStrategyKey, ViewFileSystem.RenameStrategy.SAME_MOUNTPOINT.toString());
        ViewFileSystem.RenameStrategy renameStrategy = ViewFileSystem.RenameStrategy.valueOf(renameStrategyValue);

  
        URI srcUri = new URI("hdfs://srcAuthority/srcPath");
        URI dstUri = new URI("hdfs://dstAuthority/dstPath");

    
        try {
            ViewFileSystem.verifyRenameStrategy(srcUri, dstUri, false, renameStrategy);
            fail("Expected an IOException or IllegalArgumentException to be thrown.");
        } catch (IOException | IllegalArgumentException e) {
            String expectedMessage = "Renames across Mount points not supported";
            assertTrue("Exception message did not match expectations.", e.getMessage().contains(expectedMessage));
        }
    }
}