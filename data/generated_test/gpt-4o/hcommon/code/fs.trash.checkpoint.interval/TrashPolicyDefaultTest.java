package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.TrashPolicyDefault;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class TrashPolicyDefaultTest {
    @Test
    public void testRunMethodWithDisabledTrashInterval() throws Exception {
        // 1. 使用API获取配置值，不要硬编码配置值

        // Create mock Configuration object
        Configuration mockConfig = Mockito.mock(Configuration.class);
        Mockito.when(mockConfig.getFloat("fs.trash.checkpoint.interval", 0))
               .thenReturn(0f); // Correct configuration key

        // 2. 准备测试条件

        // Create mock FileSystem object
        FileSystem mockFileSystem = Mockito.mock(FileSystem.class);

        // Initialize TrashPolicyDefault with mocked Configuration and FileSystem
        TrashPolicyDefault trashPolicy = new TrashPolicyDefault();
        trashPolicy.initialize(mockConfig, mockFileSystem);

        // Fetch the Emptier Runnable
        Runnable emptier = trashPolicy.getEmptier();

        // Mock dependencies needed within the 'run' method
        Mockito.when(mockFileSystem.getTrashRoots(Mockito.anyBoolean()))
               .thenReturn(Collections.<FileStatus>emptyList()); // Correct type for generics

        // 3. 测试代码

        // Execute the 'run' method of the Emptier
        emptier.run();

        // 4. 测试后的验证代码

        // Verify no trash processing methods are invoked
        Mockito.verify(mockFileSystem, Mockito.never()).delete(Mockito.any(Path.class), Mockito.anyBoolean());
        Mockito.verify(mockFileSystem, Mockito.never()).mkdirs(Mockito.any(Path.class));
    }
}