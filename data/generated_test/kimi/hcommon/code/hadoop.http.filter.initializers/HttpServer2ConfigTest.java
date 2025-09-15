package org.apache.hadoop.http;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URI;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ReflectionUtils.class})
public class HttpServer2ConfigTest {

    @Test
    public void testMultipleCustomInitializersAreLoadedInOrder() throws Exception {
        // 1. Configuration as input
        Configuration conf = new Configuration(false);
        String initializerList = "org.apache.hadoop.http.HttpServer2ConfigTest$FirstFilterInit,"
                               + "org.apache.hadoop.http.HttpServer2ConfigTest$SecondFilterInit,"
                               + "org.apache.hadoop.http.HttpServer2ConfigTest$ThirdFilterInit";
        conf.set("hadoop.http.filter.initializers", initializerList);

        // 2. Prepare the test conditions.
        PowerMockito.mockStatic(ReflectionUtils.class);

        FirstFilterInit firstSpy = PowerMockito.spy(new FirstFilterInit());
        SecondFilterInit secondSpy = PowerMockito.spy(new SecondFilterInit());
        ThirdFilterInit thirdSpy = PowerMockito.spy(new ThirdFilterInit());

        PowerMockito.when(ReflectionUtils.newInstance(eq(FirstFilterInit.class), any(Configuration.class)))
                .thenReturn(firstSpy);
        PowerMockito.when(ReflectionUtils.newInstance(eq(SecondFilterInit.class), any(Configuration.class)))
                .thenReturn(secondSpy);
        PowerMockito.when(ReflectionUtils.newInstance(eq(ThirdFilterInit.class), any(Configuration.class)))
                .thenReturn(thirdSpy);

        // 3. Test code.
        HttpServer2 server = new HttpServer2.Builder()
                .setName("test")
                .addEndpoint(new URI("http://localhost:0"))
                .setFindPort(true)
                .setConf(conf)
                .build();

        // 4. Code after testing.
        InOrder inOrder = inOrder(firstSpy, secondSpy, thirdSpy);
        inOrder.verify(firstSpy).initFilter(any(FilterContainer.class), any(Configuration.class));
        inOrder.verify(secondSpy).initFilter(any(FilterContainer.class), any(Configuration.class));
        inOrder.verify(thirdSpy).initFilter(any(FilterContainer.class), any(Configuration.class));

        PowerMockito.verifyStatic(times(1));
        ReflectionUtils.newInstance(eq(FirstFilterInit.class), any(Configuration.class));
        PowerMockito.verifyStatic(times(1));
        ReflectionUtils.newInstance(eq(SecondFilterInit.class), any(Configuration.class));
        PowerMockito.verifyStatic(times(1));
        ReflectionUtils.newInstance(eq(ThirdFilterInit.class), any(Configuration.class));
    }

    public static class FirstFilterInit extends FilterInitializer {
        @Override
        public void initFilter(FilterContainer container, Configuration conf) {}
    }

    public static class SecondFilterInit extends FilterInitializer {
        @Override
        public void initFilter(FilterContainer container, Configuration conf) {}
    }

    public static class ThirdFilterInit extends FilterInitializer {
        @Override
        public void initFilter(FilterContainer container, Configuration conf) {}
    }
}