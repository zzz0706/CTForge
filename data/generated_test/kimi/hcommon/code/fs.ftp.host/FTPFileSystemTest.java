package org.apache.hadoop.fs.ftp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.commons.net.ftp.FTPClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FTPClient.class, FTPFileSystem.class})
public class FTPFileSystemTest {

    @Test
    public void openFailsWhenHostUnreachable() throws Exception {
        // 1. Obtain configuration values
        Configuration conf = new Configuration();
        // rely on external test resource overrides (no conf.set(...))

        // 2. Prepare test conditions
        FTPClient mockClient = PowerMockito.mock(FTPClient.class);
        PowerMockito.whenNew(FTPClient.class).withNoArguments().thenReturn(mockClient);
        PowerMockito.doThrow(new ConnectException("Connection refused"))
                    .when(mockClient).connect("255.255.255.255", 21);

        URI uri = URI.create("ftp://255.255.255.255");
        FTPFileSystem ftpFS = new FTPFileSystem();
        ftpFS.initialize(uri, conf);

        // 3. Test code
        try {
            FSDataInputStream in = ftpFS.open(new Path("/file"), 4096);
            fail("Expected IOException due to unreachable host");
        } catch (IOException e) {
            // 4. Assertions
            assertTrue("Exception should contain connection failure message",
                       e.getMessage().contains("Failed to connect") || e.getMessage().contains("Connection refused"));
        }
    }
}