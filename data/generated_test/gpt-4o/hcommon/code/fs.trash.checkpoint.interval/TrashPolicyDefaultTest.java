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

        // Create mock Configuration object
        Configuration mockConfig = Mockito.mock(Configuration.class);
        Mockito.when(mockConfig.getFloat("fs.trash.checkpoint.interval", 0))
               .thenReturn(0f); // Correct configuration key


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


        // Execute the 'run' method of the Emptier
        emptier.run();

        // Verify no trash processing methods are invoked
        Mockito.verify(mockFileSystem, Mockito.never()).delete(Mockito.any(Path.class), Mockito.anyBoolean());
        Mockito.verify(mockFileSystem, Mockito.never()).mkdirs(Mockito.any(Path.class));
    }
}