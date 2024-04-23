package com.hmdp.utils.lock;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.BooleanUtil;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    private String name;
    private StringRedisTemplate stringRedisTemplate;

    private static final String LOCK_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true)+"-";
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public boolean tryLock(long timeoutSec) {
        //当前线程标识
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(LOCK_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(success);
    }

    public void unlock() {
        //调用lua脚本
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(LOCK_PREFIX + name),
                ID_PREFIX + Thread.currentThread().getId());
    }

//    public void unlock() {
//        //当前线程标识
//        String threadId = ID_PREFIX + Thread.currentThread().getId();
//        //获取锁中标识
//        String id = stringRedisTemplate.opsForValue().get(LOCK_PREFIX + name);
//        //判断标识是否一致
//        if(threadId.equals(id)){
//            //一致,释放锁
//            stringRedisTemplate.delete(LOCK_PREFIX + name);
//        }
//    }
}
