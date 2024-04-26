package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    @Autowired
    private CacheClient cacheClient;
    /**
     * 根据id查询商铺信息
     * @param id 商铺id
     * @return
     */
    @Override
    public Result queryById(Long id) {
        //解决缓存穿透
        //Shop shop = queryWithPassThrough(id);
        Shop shop = cacheClient
                .queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 互斥锁解决缓存击穿
        // Shop shop = cacheClient
        //         .queryWithMutex(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 逻辑过期解决缓存击穿
//         Shop shop = cacheClient
//                 .queryWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);

        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        //返回
        return Result.ok(shop);
    }

//    /**
//     * 解决缓存穿透
//     */
//    public Shop queryWithPassThrough(Long id){
//        //1、查询redis中是否存在数据
//        String shopKey = RedisConstants.CACHE_SHOP_KEY + id;
//        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
//        if (StrUtil.isNotBlank(shopJson)) {
//            //2.1、存在，返回数据
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        //判断是否命中空值
//        if(shopJson!=null){
//            return null;
//        }
//        //2.2、不存在，查询数据库
//        Shop shop = getById(id);
//        //3.1、数据库中也不存在，返回404
//        if(shop==null){
//            //在redis中缓存null,解决缓存穿透问题
//            stringRedisTemplate.opsForValue().set(shopKey,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
//            return null;
//        }
//        //3.2、将查询结果写入redis
//        shopJson = JSONUtil.toJsonStr(shop);
//        //添加随机ttl，缓解缓存雪崩问题
//        stringRedisTemplate.opsForValue().set(shopKey,shopJson,RedisConstants.CACHE_SHOP_TTL+ RandomUtil.randomLong(5),TimeUnit.MINUTES);
//        //4、返回
//        return shop;
//    }
//
//    /**
//     * 互斥锁解决缓存击穿
//     */
//    public Shop queryWithMutex(Long id){
//        //1、查询redis中是否存在数据
//        String shopKey = RedisConstants.CACHE_SHOP_KEY + id;
//        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
//        if (StrUtil.isNotBlank(shopJson)) {
//            //2.1、存在，返回数据
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        //判断是否命中空值
//        if(shopJson!=null){
//            return null;
//        }
//        //2、实现缓存重构
//        //2.1、尝试获取锁
//        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
//        Shop shop = null;
//        try {
//            boolean isLocked = tryLock(lockKey);
//            //2.1、获取失败，等待
//            if(!isLocked){
//                Thread.sleep(50);
//                return queryWithMutex(id);
//            }
//            //查询redis中是否已存在数据 DoubleCheck
//            shopJson = stringRedisTemplate.opsForValue().get(shopKey);
//            if (StrUtil.isNotBlank(shopJson)) {
//                //2.1、存在，返回数据
//                return JSONUtil.toBean(shopJson, Shop.class);
//            }
//            //判断是否命中空值
//            if(shopJson!=null){
//                return null;
//            }
//            //2.2、获取成功，查询数据库
//            shop = getById(id);
//            //Thread.sleep(200);
//            //3.1、数据库中也不存在，返回404
//            if(shop==null){
//                //在redis中缓存null,解决缓存穿透问题
//                stringRedisTemplate.opsForValue().set(shopKey,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
//                return null;
//            }
//            //3.2、将查询结果写入redis
//            shopJson = JSONUtil.toJsonStr(shop);
//            //添加随机ttl，缓解缓存雪崩问题
//            stringRedisTemplate.opsForValue().set(shopKey,shopJson,RedisConstants.CACHE_SHOP_TTL+ RandomUtil.randomLong(5),TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }finally {
//            //释放锁
//            unlock(lockKey);
//        }
//        //4、返回
//        return shop;
//    }
//
//    //尝试获取互斥锁
//    private boolean tryLock(String key){
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
//        return BooleanUtil.isTrue(flag);
//    }
//
//    //释放互斥锁
//    private void unlock(String key){
//        stringRedisTemplate.delete(key);
//    }
//
//    /**
//     * 逻辑过期解决缓存击穿
//     */
//    public Shop queryWithLogicalExpire(Long id){
//        //1、查询redis中是否存在数据
//        String shopKey = RedisConstants.CACHE_SHOP_KEY + id;
//        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
//        if (StrUtil.isBlank(shopJson)) {
//            //不存在，返回null
//            return null;
//        }
//        //2、存在，判断是否过期
//        //2.1、反序列化JSON
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        //2.2、未过期，直接返回
//        if(expireTime.isAfter(LocalDateTime.now())){
//            return shop;
//        }
//        //2.3、过期，尝试获取锁
//        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
//        boolean isLocked = tryLock(lockKey);
//        if(isLocked){
//            //查询redis中是否已过期
//            shopJson = stringRedisTemplate.opsForValue().get(shopKey);
//            if (StrUtil.isBlank(shopJson)) {
//                //不存在，返回null
//                return null;
//            }
//            redisData = JSONUtil.toBean(shopJson, RedisData.class);
//            shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
//            expireTime = redisData.getExpireTime();
//            //未过期，直接返回
//            if(expireTime.isAfter(LocalDateTime.now())){
//                return shop;
//            }
//
//            //3.1、获取成功，创建新线程完成缓存重建
//            CACHE_REBUILD_EXECUTOR.submit(()->{
//                //重建缓存
//                try {
//                    this.saveShop2Redis(id,20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }finally {
//                    //释放锁
//                    unlock(lockKey);
//                }
//            });
//        }
//        //3.2、返回过期的店铺信息
//        return shop;
//    }
//
//    //将店铺信息封装逻辑过期时间存入redis
//    public void saveShop2Redis(Long id , Long expireSeconds){
//        //1、查询数据库
//        Shop shop = getById(id);
//        try {
//            Thread.sleep(200);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        //2、封装逻辑过期时间
//        RedisData redisData = new RedisData();
//        redisData.setData(shop);
//        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
//        //3、写入redis
//        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
//    }

    /**
     * 更新店铺信息
     * @param shop
     * @return
     */
    @Override
    @Transactional
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺Id不能为空");
        }
        //1、更新数据库
        updateById(shop);
        //2、删除redis缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY+id);
        return Result.ok();
    }

    /**
     * 根据类型查询店铺，支持按距离排序
     * @param typeId
     * @param current
     * @param x
     * @param y
     * @return
     */
    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1、判断是否需要按距离排序
        if(x == null || y == null){
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query().eq("type_id",typeId)
                    .page(new Page<Shop>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }

        //2、准备分页参数
        int from = (current-1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        //3、根据typeId从redis中获取店铺id和distance
        String key = RedisConstants.SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),  //圆心
                new Distance(5000),  //半径 5000m
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)  //包含距离
        );
        if(results == null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if(list.size()<=from){
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }

        //4、解析数据,获取店铺id和distance
        List<Long> ids = new ArrayList<>(list.size());
        Map<String,Distance> map = new HashMap<>(list.size());
        //4.1、截取from~end的部分
        list.stream().skip(from).forEach(result->{
            String shopId = result.getContent().getName();
            ids.add(Long.valueOf(shopId));
            map.put(shopId,result.getDistance());
        });

        //5、查询店铺信息，封装返回结果
        String idsStr = StrUtil.join(",",ids);
        List<Shop> shops = query().in("id", ids).last("order by field(id," + idsStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(map.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shops);
    }
}
