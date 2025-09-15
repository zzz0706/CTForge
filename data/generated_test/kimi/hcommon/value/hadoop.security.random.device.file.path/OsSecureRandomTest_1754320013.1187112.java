package org.apache.hadoop.crypto.random;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({OsSecureRandom.class, FileInputStream.class, IOUtils.class})
public class OsSecureRandomTest {

    private OsSecureRandom secureRandom;

    @Mock
    private FileInputStream mockedStream;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        secureRandom = new OsSecureRandom();
    }

    @After
    public void tearDown() {
        secureRandom.close();
    }

    @Test
    public void testConfigurationCustomPath() throws Exception {
        // 1. Prepare the test conditions
        Configuration conf = new Configuration();
        String customPath = "/dev/customRandom";
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY, customPath);

        // 2. Set up mocks
        PowerMockito.whenNew(FileInputStream.class)
                   .withArguments(new File(customPath))
                   .thenReturn(mockedStream);
        PowerMockito.mockStatic(IOUtils.class);
        PowerMockito.doNothing()
                    .when(IOUtils.class);
        IOUtils.readFully(any(FileInputStream.class), any(byte[].class), anyInt(), anyInt());

        // 3. Test code
        secureRandom.setConf(conf);
        byte[] buffer = new byte[16];
        secureRandom.nextBytes(buffer);

        // 4. Verify
        PowerMockito.verifyNew(FileInputStream.class).withArguments(new File(customPath));
        assertNotNull(buffer);
    }

    @Test
    public void testConfigurationDefaultPath() throws Exception {
        // 1. Prepare the test conditions
        Configuration conf = new Configuration(); // key not set

        // 2. Compute expected default
        String defaultPath = conf.get(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY,
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT);

        // 3. Set up mocks
        PowerMockito.whenNew(FileInputStream.class)
                   .withArguments(new File(defaultPath))
                   .thenReturn(mockedStream);
        PowerMockito.mockStatic(IOUtils.class);
        PowerMockito.doNothing()
                    .when(IOUtils.class);
        IOUtils.readFully(any(FileInputStream.class), any(byte[].class), anyInt(), anyInt());

        // 4. Test code
        secureRandom.setConf(conf);
        byte[] buffer = new byte[16];
        secureRandom.nextBytes(buffer);

        // 5. Verify
        PowerMockito.verifyNew(FileInputStream.class).withArguments(new File(defaultPath));
    }

    @Test
    public void testEmptyPathFallsBackToDefault() throws Exception {
        // 1. Prepare the test conditions
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY, "");

        // 2. Compute expected default (empty string is returned as-is)
        String expectedPath = "";

        // 3. Set up mocks
        PowerMockito.whenNew(FileInputStream.class)
                   .withArguments(new File(expectedPath))
                   .thenReturn(mockedStream);
        PowerMockito.mockStatic(IOUtils.class);
        PowerMockito.doNothing()
                    .when(IOUtils.class);
        IOUtils.readFully(any(FileInputStream.class), any(byte[].class), anyInt(), anyInt());

        // 4. Test code
        secureRandom.setConf(conf);
        byte[] buffer = new byte[16];
        secureRandom.nextBytes(buffer);

        // 5. Verify
        PowerMockito.verifyNew(FileInputStream.class).withArguments(new File(expectedPath));
    }

    @Test
    public void testIOExceptionOnOpen() throws Exception {
        // 1. Prepare the test conditions
        Configuration conf = new Configuration();
        String badPath = "/bad/path";
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY, badPath);

        // 2. Set up mocks to throw IOException
        PowerMockito.whenNew(FileInputStream.class)
                   .withArguments(new File(badPath))
                   .thenThrow(new IOException("No such file"));

        // 3. Test code
        secureRandom.setConf(conf);
        try {
            secureRandom.nextBytes(new byte[16]);
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            // 4. Verify
            assertEquals("failed to fill reservoir", e.getMessage());
            assertTrue(e.getCause() instanceof IOException);
        }
    }

    @Test
    public void testLargeByteArrayTriggersMultipleFills() throws Exception {
        // 1. Prepare the test conditions
        Configuration conf = new Configuration();
        String customPath = "/dev/urandom";
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY, customPath);

        // 2. Set up mocks
        PowerMockito.whenNew(FileInputStream.class)
                   .withArguments(new File(customPath))
                   .thenReturn(mockedStream);
        PowerMockito.mockStatic(IOUtils.class);
        PowerMockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                byte[] b = (byte[]) invocation.getArguments()[1];
                java.util.Arrays.fill(b, (byte) 0xAA);
                return null;
            }
        }).when(IOUtils.class);
        IOUtils.readFully(any(FileInputStream.class), any(byte[].class), anyInt(), anyInt());

        // 3. Test code
        secureRandom.setConf(conf);
        byte[] largeBuffer = new byte[8192];
        secureRandom.nextBytes(largeBuffer);

        // 4. Verify
        PowerMockito.verifyNew(FileInputStream.class, times(1)).withArguments(new File(customPath));
        PowerMockito.verifyStatic(times(1));
        IOUtils.readFully(any(FileInputStream.class), any(byte[].class), anyInt(), anyInt());
    }

    @Test
    public void testMultipleNextBytesReuseSameStream() throws Exception {
        // 1. Prepare the test conditions
        Configuration conf = new Configuration();
        String customPath = "/dev/urandom";
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY, customPath);

        // 2. Set up mocks
        PowerMockito.whenNew(FileInputStream.class)
                   .withArguments(new File(customPath))
                   .thenReturn(mockedStream);
        PowerMockito.mockStatic(IOUtils.class);
        PowerMockito.doNothing()
                    .when(IOUtils.class);
        IOUtils.readFully(any(FileInputStream.class), any(byte[].class), anyInt(), anyInt());

        // 3. Test code
        secureRandom.setConf(conf);
        secureRandom.nextBytes(new byte[16]);
        secureRandom.nextBytes(new byte[16]);

        // 4. Verify
        PowerMockito.verifyNew(FileInputStream.class, times(1)).withArguments(new File(customPath));
    }
}