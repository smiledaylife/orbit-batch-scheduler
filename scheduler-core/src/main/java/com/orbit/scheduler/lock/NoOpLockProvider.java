package com.orbit.scheduler.lock;

import com.orbit.scheduler.spi.LockProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 无锁实现（单机部署 / 测试场景）。
 *
 * @author orbit
 */
public class NoOpLockProvider implements LockProvider {

    private static final Logger log = LoggerFactory.getLogger(NoOpLockProvider.class);

    @Override
    public boolean tryLock(String key, String owner, Duration lease) {
        return true;
    }

    @Override
    public boolean renew(String key, String owner, Duration lease) {
        return true;
    }

    @Override
    public void unlock(String key, String owner) {
        // no-op
    }

    @Override
    public String type() {
        return "none";
    }
}
