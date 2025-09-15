package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CryptoCodec.class, ReflectionUtils.class})
public class CryptoCodecConfigurationTest {

    @Test
    public void testCustomCipherSuiteOverridesDefault() throws Exception {
        // 1. Use HDFS 2.8.5 API to set the custom cipher suite via Configuration
        Configuration conf = new Configuration();
        String customSuite = "AES/CTR/NoPadding";
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY, customSuite);

        // 2. Prepare the test conditions: mock the codec class list and codec instance
        CryptoCodec mockCodec = mock(CryptoCodec.class);
        when(mockCodec.getCipherSuite()).thenReturn(CipherSuite.AES_CTR_NOPADDING);

        // Mock static getCodecClasses to return list containing mock codec class
        PowerMockito.spy(CryptoCodec.class);
        List<Class<? extends CryptoCodec>> mockList = Arrays.<Class<? extends CryptoCodec>>asList(mockCodec.getClass());
        PowerMockito.doReturn(mockList)
                .when(CryptoCodec.class, "getCodecClasses", conf, CipherSuite.AES_CTR_NOPADDING);

        // Mock ReflectionUtils to return the mock codec instance
        PowerMockito.mockStatic(ReflectionUtils.class);
        PowerMockito.when(ReflectionUtils.newInstance(mockCodec.getClass(), conf))
                .thenReturn(mockCodec);

        // 3. Test code: invoke the public method under test
        CryptoCodec result = CryptoCodec.getInstance(conf);

        // 4. Code after testing: assertions and verification
        assertNotNull("Returned CryptoCodec must not be null", result);
        assertEquals("Cipher suite must match configured value",
                CipherSuite.AES_CTR_NOPADDING, result.getCipherSuite());
    }
}