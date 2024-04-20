package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


@Component
public class CacheClient {

    private StringRedisTemplate stringRedisTemplate;
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    // 将任意Java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    // 将任意Java对象序列化为json并存储在string类型的key中，并且可以设置逻辑过期时间，用于处理缓存击穿问题
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        //封装过期时间
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //存入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    // 根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R>dbFallback,Long time, TimeUnit unit){
        //1、查询redis中是否存在数据
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            //2.1、存在，返回数据
            return JSONUtil.toBean(json, type);
        }
        //判断是否命中空值
        if(json!=null){
            return null;
        }
        //2.2、不存在，查询数据库
        R r = dbFallback.apply(id);
        //3.1、数据库中也不存在，返回404
        if(r==null){
            //在redis中缓存null,解决缓存穿透问题
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //3.2、将查询结果写入redis
        //添加随机ttl，缓解缓存雪崩问题
        this.set(key,r,time+RandomUtil.randomLong(5),unit);
        //4、返回
        return r;
    }

    //尝试获取互斥锁
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    //释放互斥锁
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    // 根据指定的key查询缓存，并反序列化为指定类型，需要利用逻辑过期解决缓存击穿问题将逻辑进行封装
    public <R,ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID,R>dbFallback,Long time, TimeUnit unit){
        //1、查询redis中是否存在数据
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isBlank(json)) {
            //不存在，返回null
            return null;
        }
        //2、存在，判断是否过期
        //2.1、反序列化JSON
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //2.2、未过期，直接返回
        if(expireTime.isAfter(LocalDateTime.now())){
            return r;
        }
        //2.3、过期，尝试获取锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLocked = tryLock(lockKey);
        if(isLocked){
            //查询redis中是否已过期
            json = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isBlank(json)) {
                //不存在，返回null
                return null;
            }
            redisData = JSONUtil.toBean(json, RedisData.class);
            r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
            expireTime = redisData.getExpireTime();
            //未过期，直接返回
            if(expireTime.isAfter(LocalDateTime.now())){
                return r;
            }

            //3.1、获取成功，创建新线程完成缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                //重建缓存
                try {
                    //查询数据库
                    R newR = dbFallback.apply(id);
                    // 重建缓存
                    this.setWithLogicalExpire(key, newR, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unlock(lockKey);
                }
            });
        }
        //3.2、返回过期的店铺信息
        return r;
    }

    // 根据指定的key查询缓存，并反序列化为指定类型，需要利用互斥锁解决缓存击穿问题将逻辑进行封装
    public <R,ID> R queryWithMutex(String keyPrefix, ID id, Class<R> type, Function<ID,R>dbFallback,Long time, TimeUnit unit){
        //1、查询redis中是否存在数据
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            //2.1、存在，返回数据
            return JSONUtil.toBean(json, type);
        }
        //判断是否命中空值
        if(json!=null){
            return null;
        }
        //2、实现缓存重构
        //2.1、尝试获取锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        R r = null;
        try {
            boolean isLocked = tryLock(lockKey);
            //2.1、获取失败，等待
            if(!isLocked){
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit);
            }
            //查询redis中是否已存在数据 DoubleCheck
            json = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(json)) {
                //2.1、存在，返回数据
                return JSONUtil.toBean(json, type);
            }
            //判断是否命中空值
            if(json!=null){
                return null;
            }
            //2.2、获取成功，查询数据库
            r = dbFallback.apply(id);
            //3.1、数据库中也不存在，返回404
            if(r==null){
                //在redis中缓存null,解决缓存穿透问题
                stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
            //3.2、将查询结果写入redis
            this.set(key,r,time+RandomUtil.randomLong(5),unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //释放锁
            unlock(lockKey);
        }
        //4、返回
        return r;
    }
}
