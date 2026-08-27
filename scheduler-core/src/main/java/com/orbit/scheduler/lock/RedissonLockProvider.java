package com.orbit.scheduler.lock;

import com.orbit.scheduler.spi.LockProvider;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Redis 分布式锁（Redisson 实现）。
 *
 * <p>采用 watchdog 模式：tryLock 指定 leaseTime=-1，锁由 Redisson 内部看门狗
 * （默认 30 秒租约、每 10 秒自动续期）维护；节点宕机后看门狗停止，
 * 锁最迟 30 秒自动释放。因此本实现的 {@link #renew} 为空操作。
 *
 * @author orbit
 */
public class RedissonLockProvider implements LockProvider {

    private static final Logger log = LoggerFactory.getLogger(RedissonLockProvider.class);

    private final RedissonClient redissonClient;

    public RedissonLockProvider(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    @Override
    public boolean tryLock(String key, String owner, Duration lease) {
        try {
            RLock lock = redissonClient.getLock(key);
            // waitTime=0 立即返回；leaseTime=-1 启用 watchdog 自动续期
            return lock.tryLock(0, -1, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            log.warn("[orbit-scheduler] redis tryLock {} failed: {}", key, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean renew(String key, String owner, Duration lease) {
        // watchdog 自动续期，无需手工处理
        return true;
    }

    @Override
    public void unlock(String key, String owner) {
        try {
            RLock lock = redissonClient.getLock(key);
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            } else {
                log.warn("[orbit-scheduler] lock {} not held by current thread (expired or taken over), skip unlock", key);
            }
        } catch (Exception e) {
            log.warn("[orbit-scheduler] redis unlock {} failed: {}", key, e.getMessage());
        }
    }

    @Override
    public String type() {
        return "redis";
    }
}
