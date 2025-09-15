package org.apache.hadoop.fs.permission;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestFsPermission {
    // Test code
    // 1. 使用API获取配置值，不要硬编码配置值
    // 2. 准备测试条件
    // 3. 测试代码
    // 4. 测试后的代码

    @Test
    public void testInvalidUmaskValueHandlingUnderWorkload() {
        Configuration configuration = new Configuration();
        
        // 准备测试条件：模拟设置无效的 umask 值并验证它的处理方式
        configuration.set("fs.permissions.umask-mode", "999"); // 无效的 umask 值

        try {
            // 获取 umask，将抛出异常
            FsPermission.getUMask(configuration);

            // 如果没有抛出异常则测试失败
            fail("Expected IllegalArgumentException not thrown for invalid umask value");
        } catch (IllegalArgumentException e) {
            // 验证异常消息是否包含无效值的细节
            assertTrue("Exception message does not indicate invalid umask value",
                       e.getMessage() != null 
                       && (e.getMessage().contains("Invalid") || e.getMessage().contains("umask"))); // 增加检查异常中的 umask 字样
        } catch (Exception e) {
            // 其他异常导致测试失败
            fail("Unexpected exception: " + e.getMessage());
        }
    }
}