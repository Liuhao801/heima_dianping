package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.User;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import com.hmdp.utils.lock.SimpleRedisLock;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    private ISeckillVoucherService voucherService;
    @Autowired
    private RedisIdWorker redisIdWorker;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    private IVoucherOrderService proxy;

    @PostConstruct
    //类初始化后执行
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable{
        private String queueName = "stream.orders";
        @Override
        public void run() {
            while(true){
                try {
                    //1、从消息队列中获取消息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2、判断订单信息是否为空
                    if(list == null || list.isEmpty()){
                        //如果为null，则没有消息，进入下一次循环
                        continue;
                    }
                    //3、解析消息，获取订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //4、创建订单
                    handleVoucherOrder(voucherOrder);
                    //5、确认ack
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    handlePendingList();
                }
            }
        }

        //处理未确认的消息
        private void handlePendingList() {
            while(true){
                try {
                    //1、从pending-list中获取消息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2、判断订单信息是否为空
                    if(list == null || list.isEmpty()){
                        //如果为null，则没有异常订单，退出循环
                        break;
                    }
                    //3、解析消息，获取订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //4、创建订单
                    handleVoucherOrder(voucherOrder);
                    //5、确认ack
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常",e);
                    try {
                        Thread.sleep(20);
                    } catch (Exception e2) {
                        e2.printStackTrace();
                    }
                }
            }
        }
    }

//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);
//    private class VoucherOrderHandler implements Runnable{
//        @Override
//        public void run() {
//            while(true){
//                try {
//                    //从阻塞对列中获取订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    //创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常",e);
//                }
//            }
//        }
//    }

    //创建订单
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //创建锁
        RLock lock = redissonClient.getLock("lock:order:" + voucherOrder.getUserId());
        //尝试获取锁 默认不等待，30s释放锁
        boolean isLocked = lock.tryLock();
        //获取锁失败
        if(!isLocked) {
            log.error("一名用户仅限购买一单！");
            return;
        }
        try {
            //获取代理对象（事务）
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            //释放锁
            lock.unlock();
        }
    }

    /**
     * 秒杀券下单
     * @param voucherId
     * @return
     */
    @Override
    @Transactional
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //订单id
        long orderId = redisIdWorker.nextId("order");
        //1、执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );
        int r = result.intValue();
        //2、判断是否具有购买资格 0：具有资格 1：库存不足 2：一名用户仅限购买一单
        if(r!=0){
            return Result.fail( r==1 ? "库存不足！":"一名用户仅限购买一单！");
        }
        //3、获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //4、返回订单id
        return Result.ok(orderId);
    }

//    /**
//     * 秒杀券下单
//     * @param voucherId
//     * @return
//     */
//    @Override
//    @Transactional
//    public Result seckillVoucher(Long voucherId) {
//        //获取用户
//        Long userId = UserHolder.getUser().getId();
//        //1、执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        int r = result.intValue();
//        //2、判断是否具有购买资格 0：具有资格 1：库存不足 2：一名用户仅限购买一单
//        if(r!=0){
//            return Result.fail( r==1 ? "库存不足！":"一名用户仅限购买一单！");
//        }
//        //3、将订单信息添加到阻塞队列
//        //3.1、创建订单信息
//        VoucherOrder voucherOrder = new VoucherOrder();
//        //订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(userId);
//        //秒杀券id
//        voucherOrder.setVoucherId(voucherId);
//        //3.2、放入阻塞队列
//        orderTasks.add(voucherOrder);
//        //4、获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        //5、返回订单id
//        return Result.ok(orderId);
//    }

    /**
     * 创建秒杀券订单
     * @param voucherOrder
     * @return
     */
    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder){
        //4、一人一单
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if(count > 0){
            log.error("一名用户仅限购买一单！");
            return;
        }
        //5、扣减库存
        boolean success = voucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock",0)  // 乐观锁
                .update();
        if(!success){
            log.error("库存不足！");
            return;
        }
        save(voucherOrder);
    }

//    /**
//     * 秒杀券下单
//     * @param voucherId
//     * @return
//     */
//    @Override
//    @Transactional
//    public Result seckillVoucher(Long voucherId) {
//        //1、查询秒杀券信息
//        SeckillVoucher seckillVoucher = voucherService.getById(voucherId);
//        if (seckillVoucher == null) {
//            return Result.fail("秒杀券不存在！");
//        }
//        //2、判断是否在秒杀时间内
//        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀尚未开始！");
//        }
//        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已经结束！");
//        }
//        //3、判断库存是否充足
//        if (seckillVoucher.getStock() < 1) {
//            return Result.fail("库存不足！");
//        }
//        //用户id
//        Long userId = UserHolder.getUser().getId();
//        //创建锁
//        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        //尝试获取锁 默认不等待，30s释放锁
//        boolean isLocked = lock.tryLock();
//        //获取锁失败
//        if(!isLocked) {
//            return Result.fail("一名用户仅限购买一单！");
//        }
//        try {
//            //获取代理对象（事务）
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            //释放锁
//            lock.unlock();
//        }
//    }

//    /**
//     * 创建秒杀券订单
//     * @param voucherId
//     * @return
//     */
//    @Override
//    @Transactional
//    public Result createVoucherOrder(Long voucherId){
//        //4、一人一单
//        Long userId = UserHolder.getUser().getId();
//        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//        if(count > 0){
//            return Result.fail("一名用户仅限购买一单！");
//        }
//        //5、扣减库存
//        boolean success = voucherService.update()
//                .setSql("stock = stock - 1")
//                .eq("voucher_id", voucherId).gt("stock",0)  // 乐观锁
//                .update();
//        if(!success){
//            return Result.fail("库存不足！");
//        }
//        //6、创建订单信息
//        VoucherOrder voucherOrder = new VoucherOrder();
//        //订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(userId);
//        //秒杀券id
//        voucherOrder.setVoucherId(voucherId);
//        save(voucherOrder);
//        //6、返回订单id
//        return Result.ok(orderId);
//    }
}
