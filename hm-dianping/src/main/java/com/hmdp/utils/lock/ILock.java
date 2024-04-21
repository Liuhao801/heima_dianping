package com.hmdp.utils.lock;

public interface ILock {

    /**
     * 尝试获取锁
     * @param timeoutSec 锁持有的超时时间，超时自动释放
     * @return true表示获取锁成功，false表示获取失败
     */
    boolean tyrLock(long timeoutSec);

    /**
     * 释放锁
     */
    void unlock();
}
