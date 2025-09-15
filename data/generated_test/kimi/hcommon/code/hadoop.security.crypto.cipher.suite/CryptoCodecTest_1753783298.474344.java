package org.apache.hadoop.crypto;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.apache.hadoop.util.ReflectionUtils;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CryptoCodec.class, ReflectionUtils.class})
public class CryptoCodecTest {

    @Test
    public void testCustomCipherSuiteOverridesDefault() throws Exception {
        // 1. Create Configuration and set the custom cipher suite
        Configuration conf = new Configuration();
        String customSuite = "AES/CTR/NoPadding";
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY, customSuite);

        // 2. Prepare mock CryptoCodec that returns the custom suite
        CryptoCodec mockCodec = mock(CryptoCodec.class);
        when(mockCodec.getCipherSuite()).thenReturn(CipherSuite.AES_CTR_NOPADDING);

        // 3. Stub static getCodecClasses to return list with mock class
        PowerMockito.spy(CryptoCodec.class);
        List<Class<? extends CryptoCodec>> mockList = Arrays.<Class<? extends CryptoCodec>>asList(mockCodec.getClass());
        PowerMockito.doReturn(mockList)
                     .when(CryptoCodec.class, "getCodecClasses", conf, CipherSuite.AES_CTR_NOPADDING);

        // 4. Stub ReflectionUtils.newInstance to return mockCodec
        PowerMockito.mockStatic(ReflectionUtils.class);
        PowerMockito.when(ReflectionUtils.newInstance(any(Class.class), eq(conf)))
                    .thenReturn(mockCodec);

        // 5. Call method under test
        CryptoCodec result = CryptoCodec.getInstance(conf);

        // 6. Assert expectations
        assertNotNull("Returned codec should not be null", result);
        assertEquals("Cipher suite should match configured value",
                     CipherSuite.AES_CTR_NOPADDING, result.getCipherSuite());
    }
}