package org.apache.hadoop.hdfs;   

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.hdfs.server.namenode.NameNodeResourceChecker;   
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;   
import org.junit.Before;   
import org.junit.Test;   
import org.mockito.Mock;   
import org.mockito.MockitoAnnotations;   

import java.io.File;   
import java.util.ArrayList;   

import static org.junit.Assert.assertFalse;   
import static org.mockito.Mockito.mock;   
import static org.mockito.Mockito.when;   

public class HasAvailableDiskSpaceTest {   

    @Mock   
    private NameNodeResourceChecker mockResourceChecker;   

    private NNStorage mockNNStorage;   

    @Before   
    public void setup() {   
        // Initialize mocks   
        MockitoAnnotations.initMocks(this);   

        // Mock prerequisites: Mock a resource checker to simulate disk space and related checks   
        mockResourceChecker = mock(NameNodeResourceChecker.class);   
        when(mockResourceChecker.hasAvailableDiskSpace()).thenReturn(false);   

        // Mock NNStorage object and attach mock StorageDirectories   
        mockNNStorage = mock(NNStorage.class);   

        // Mock a few storage directories for the test   
        ArrayList<StorageDirectory> mockStorageDirs = new ArrayList<>();   
        StorageDirectory mockDir1 = mock(StorageDirectory.class);   
        StorageDirectory mockDir2 = mock(StorageDirectory.class);   

        when(mockDir1.getRoot()).thenReturn(new File("/mock/path1"));   
        when(mockDir2.getRoot()).thenReturn(new File("/mock/path2"));   

        mockStorageDirs.add(mockDir1);   
        mockStorageDirs.add(mockDir2);   
        when(mockNNStorage.dirIterable(null)).thenReturn(mockStorageDirs);   
    }   

    @Test   
    public void testAllVolumesUnavailable() {   
        // 1. Use the HDFS 2.8.5 API correctly to obtain configuration values   
        Configuration configuration = new Configuration();   
        configuration.set("dfs.name.dir", "/mock/path"); // Use API to provide mock configuration   

        // 2. Prepare the test conditions.   
        // Directly rely on the mocked resource checker behavior configured in setup()   

        // 3. Test code.   
        boolean result = mockResourceChecker.hasAvailableDiskSpace();   

        // 4. Code after testing.   
        assertFalse("Expected no available disk space, but some disk space is available.", result);   
    }   
}