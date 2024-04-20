package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    private ISeckillVoucherService voucherService;
    @Autowired
    private RedisIdWorker redisIdWorker;

    /**
     * 秒杀券下单
     * @param voucherId
     * @return
     */
    @Override
    @Transactional
    public Result seckillVoucher(Long voucherId) {
        //1、查询秒杀券信息
        SeckillVoucher seckillVoucher = voucherService.getById(voucherId);
        if (seckillVoucher == null) {
            return Result.fail("秒杀券不存在！");
        }
        //2、判断是否在秒杀时间内
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始！");
        }
        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束！");
        }
        //3、判断库存是否充足
        if (seckillVoucher.getStock() < 1) {
            return Result.fail("库存不足！");
        }
        //用户id
        Long userId = UserHolder.getUser().getId();
        synchronized (userId.toString().intern()){
            //获取代理对象（事务）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }
    }

    /**
     * 创建秒杀券订单
     * @param voucherId
     * @return
     */
    @Override
    @Transactional
    public Result createVoucherOrder(Long voucherId){
        //4、一人一单
        Long userId = UserHolder.getUser().getId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if(count > 0){
            return Result.fail("一名用户仅限购买一单！");
        }
        //5、扣减库存
        boolean success = voucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock",0)  // 乐观锁
                .update();
        if(!success){
            return Result.fail("库存不足！");
        }
        //6、创建订单信息
        VoucherOrder voucherOrder = new VoucherOrder();
        //订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        //秒杀券id
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);
        //6、返回订单id
        return Result.ok(orderId);
    }
}
