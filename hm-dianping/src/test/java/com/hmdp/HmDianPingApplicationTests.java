package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@SpringBootTest
class HmDianPingApplicationTests {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private ShopServiceImpl shopService;
    @Autowired
    private RedisIdWorker redisIdWorker;

    private ExecutorService es = Executors.newFixedThreadPool(500);
    @Test
    void test() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(()->{
                for (int j = 0; j < 100; j++) {
                    long count = redisIdWorker.nextId("order");
                    System.out.println(count);
                }
                latch.countDown();
            });
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println(end-begin);
    }

    @Test
    void loadShop(){
        //查询店铺信息
        List<Shop> list = shopService.list();
        //按typeId分类
        Map<Long, List<Shop>> listMap = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        //存入redis
        for (Map.Entry<Long, List<Shop>> entry : listMap.entrySet()) {
            Long typeId = entry.getKey();
            List<Shop> shops = entry.getValue();

            String key = RedisConstants.SHOP_GEO_KEY +typeId;
            List<RedisGeoCommands.GeoLocation<String>> locations = shops.stream()
                            .map(shop -> new RedisGeoCommands.GeoLocation<>(shop.getId().toString(), new Point(shop.getX(), shop.getY())))
                            .collect(Collectors.toList());
            //批量存入
            stringRedisTemplate.opsForGeo().add(key,locations);
        }
    }
}
