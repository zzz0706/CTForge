package org.apache.hadoop.fs.ftp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.IOException;
import org.mockito.Mockito;
import java.net.URI;

public class TestFTPFileSystem {

    @Test
    public void test_getFileStatus_validFile() throws IOException {
     
        Configuration conf = new Configuration();
        FTPFileSystem ftpFileSystem = Mockito.spy(new FTPFileSystem());

      
        Mockito.doNothing().when(ftpFileSystem).initialize(Mockito.any(URI.class), Mockito.eq(conf));
        FileStatus mockFileStatus = new FileStatus(123L, false, 1, 1024L, 1680201223000L, new Path("/path/to/test/file.txt"));
        Mockito.doReturn(mockFileStatus).when(ftpFileSystem).getFileStatus(Mockito.any(Path.class));

   
        conf.set("fs.ftp.host", "127.0.0.1"); 
        conf.set("fs.ftp.user", "testuser");
        conf.set("fs.ftp.password", "testpassword");
        Path testFilePath = new Path("/path/to/test/file.txt");

        ftpFileSystem.initialize(URI.create("ftp://127.0.0.1"), conf);
        FileStatus fileStatus = ftpFileSystem.getFileStatus(testFilePath);
        
        assertNotNull("FileStatus object should not be null", fileStatus);
        assertEquals("Returned path should match the input path",
                testFilePath.getName(), fileStatus.getPath().getName());
        assertFalse("File status should not report as a directory", fileStatus.isDirectory());
        assertEquals("File length should match", 123L, fileStatus.getLen());
    }
}