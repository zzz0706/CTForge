package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.server.namenode.FSDirAttrOp;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class FSDirAttrOpTest {

    private FSDirectory fsdMock;
    private INodesInPath iipMock;
    private INode inodeMock;

    @Before
    public void setUp() throws Exception {
        // 1. Prepare mock objects
        fsdMock = Mockito.mock(FSDirectory.class);
        iipMock = Mockito.mock(INodesInPath.class);
        inodeMock = Mockito.mock(INode.class);

        // 2. Prepare mock behavior
        Mockito.when(fsdMock.hasWriteLock()).thenReturn(true);
        Mockito.when(fsdMock.getAccessTimePrecision()).thenReturn(100L); // Using the HDFS API configuration method
        Mockito.when(iipMock.getLastINode()).thenReturn(inodeMock);
        Mockito.when(iipMock.getLatestSnapshotId()).thenReturn(1);
        Mockito.when(inodeMock.getAccessTime()).thenReturn(200L);
    }

    @Test
    public void testAtimeNotWithinPrecisionInterval() throws Exception {
        // 3. Prepare test conditions
        long mtime = -1L; // No modification time
        long atime = 400L; // Test: atime > inode.accessTime + precision
        boolean force = false; // Force not used in this test case

        // 4. Mock specific method behavior
        Mockito.doNothing().when(inodeMock).setAccessTime(atime, 1);
        
        // 5. Execute the function to test
        boolean status = FSDirAttrOp.unprotectedSetTimes(fsdMock, iipMock, mtime, atime, force);

        // 6. Verify results
        Mockito.verify(inodeMock).setAccessTime(atime, 1);
        org.junit.Assert.assertTrue("Expected status to be true", status);
    }
}