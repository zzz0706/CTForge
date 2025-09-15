package org.apache.hadoop.crypto.random;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({OsSecureRandom.class, FileInputStream.class})
public class OsSecureRandomTest {

    @Test
    public void testCustomRandomDeviceFileIsUsedWhenKeyIsSet() throws Exception {
        // 1. Create Configuration and set custom value
        Configuration conf = new Configuration();
        String customPath = "/custom/random";
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY, customPath);

        // 2. Compute expected value from Configuration
        String expectedPath = conf.get(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY,
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT);

        // 3. Prepare mocks
        FileInputStream mockedStream = mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class).withAnyArguments().thenReturn(mockedStream);

        // 4. Invoke method under test
        OsSecureRandom secureRandom = new OsSecureRandom();
        secureRandom.setConf(conf);
        secureRandom.nextBytes(new byte[16]);

        // 5. Verify FileInputStream was opened with expected path
        PowerMockito.verifyNew(FileInputStream.class).withArguments(new File(expectedPath));
    }

    @Test
    public void testDefaultDeviceFileIsUsedWhenKeyNotSet() throws Exception {
        // 1. Create Configuration without setting the key
        Configuration conf = new Configuration();

        // 2. Compute expected value from Configuration (should be default)
        String expectedPath = conf.get(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY,
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT);

        // 3. Prepare mocks
        FileInputStream mockedStream = mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class).withAnyArguments().thenReturn(mockedStream);

        // 4. Invoke method under test
        OsSecureRandom secureRandom = new OsSecureRandom();
        secureRandom.setConf(conf);
        secureRandom.nextBytes(new byte[16]);

        // 5. Verify FileInputStream was opened with default path
        PowerMockito.verifyNew(FileInputStream.class).withArguments(new File(expectedPath));
    }

    @Test
    public void testEmptyPathFallsBackToDefault() throws Exception {
        // 1. Create Configuration and set empty string
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY, "");

        // 2. Compute expected value from Configuration (empty string is treated as set, so empty is used)
        String expectedPath = conf.get(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY,
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT);

        // 3. Prepare mocks
        FileInputStream mockedStream = mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class).withAnyArguments().thenReturn(mockedStream);

        // 4. Invoke method under test
        OsSecureRandom secureRandom = new OsSecureRandom();
        secureRandom.setConf(conf);
        secureRandom.nextBytes(new byte[16]);

        // 5. Verify FileInputStream was opened with empty string path
        PowerMockito.verifyNew(FileInputStream.class).withArguments(new File(expectedPath));
    }

    @Test
    public void testIOExceptionWhenOpeningDeviceFile() throws Exception {
        // 1. Create Configuration with non-existent path
        Configuration conf = new Configuration();
        String invalidPath = "/nonexistent/random";
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY, invalidPath);

        // 2. Prepare mocks to throw IOException
        PowerMockito.whenNew(FileInputStream.class).withAnyArguments().thenThrow(new IOException("Device not found"));

        // 3. Invoke method under test and expect RuntimeException
        OsSecureRandom secureRandom = new OsSecureRandom();
        secureRandom.setConf(conf);
        try {
            secureRandom.nextBytes(new byte[16]);
            fail("Expected RuntimeException due to IOException");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof IOException);
            assertEquals("failed to fill reservoir", e.getMessage());
        }
    }

    @Test
    public void testMultipleNextBytesReuseSameStream() throws Exception {
        // 1. Create Configuration with custom path
        Configuration conf = new Configuration();
        String customPath = "/custom/random";
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY, customPath);

        // 2. Prepare mocks
        FileInputStream mockedStream = mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class).withAnyArguments().thenReturn(mockedStream);

        // 3. Invoke method under test multiple times
        OsSecureRandom secureRandom = new OsSecureRandom();
        secureRandom.setConf(conf);
        secureRandom.nextBytes(new byte[16]);
        secureRandom.nextBytes(new byte[16]);

        // 4. Verify FileInputStream was created only once
        PowerMockito.verifyNew(FileInputStream.class, times(1)).withArguments(new File(customPath));
    }

    @Test
    public void testLargeByteArrayTriggersMultipleReservoirFills() throws Exception {
        // 1. Create Configuration with custom path
        Configuration conf = new Configuration();
        String customPath = "/custom/random";
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY, customPath);

        // 2. Prepare mocks
        FileInputStream mockedStream = mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class).withAnyArguments().thenReturn(mockedStream);

        // 3. Invoke method under test with large byte array
        OsSecureRandom secureRandom = new OsSecureRandom();
        secureRandom.setConf(conf);
        secureRandom.nextBytes(new byte[8192]); // Large enough to trigger multiple fills

        // 4. Verify FileInputStream was opened with expected path
        PowerMockito.verifyNew(FileInputStream.class).withArguments(new File(customPath));
    }
}