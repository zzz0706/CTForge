package org.apache.hadoop.fs;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

public class FileSystemCreateRespectsBufferSizeConfigTest {

    @Test
    public void testFileSystemCreateRespectsBufferSizeConfig() throws IOException {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration(false); // do not load defaults from classpath
        conf.addResource("core-site.xml");
        final int expectedBufferSize = conf.getInt(
                CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
                CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);

        // 2. Prepare the test conditions.
        FileSystem fs = spy(new RawLocalFileSystem());
        fs.initialize(new Path("file:///").toUri(), conf);

        final int[] capturedBufferSize = new int[1];
        doAnswer(new org.mockito.stubbing.Answer<FSDataOutputStream>() {
            @Override
            public FSDataOutputStream answer(org.mockito.invocation.InvocationOnMock invocation) throws Throwable {
                // bufferSize is the 4th argument (index 3) in the 8-arg create method
                capturedBufferSize[0] = (Integer) invocation.getArguments()[3];
                return mock(FSDataOutputStream.class);
            }
        }).when(fs).create(any(Path.class),
                           any(FsPermission.class),
                           any(EnumSet.class), // CreateFlag set
                           anyInt(),           // bufferSize
                           anyShort(),         // replication
                           anyLong(),          // blockSize
                           any(Progressable.class),
                           any(Options.ChecksumOpt.class));

        // 3. Test code.
        fs.create(new Path("/tmp/test"),
                  FsPermission.getFileDefault(),
                  EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
                  expectedBufferSize, // ensure we pass the configured buffer size
                  (short) 1,
                  64 * 1024 * 1024,
                  null,
                  null);

        // 4. Code after testing.
        assertEquals("bufferSize should match configuration",
                     expectedBufferSize, capturedBufferSize[0]);
    }
}