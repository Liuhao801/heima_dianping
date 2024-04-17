package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.xml.transform.sax.SAXResult;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 查询店铺类型列表
     * @return
     */
    @Override
    public Result queryTypeList() {
        //1、查询redis
        String key = RedisConstants.CACHE_SHOP_LIST_KEY;
        String typeListStr = stringRedisTemplate.opsForValue().get(key);
        //2、查到，直接返回
        if(StrUtil.isNotBlank(typeListStr)){
            List<ShopType> typeList = JSONUtil.toList(typeListStr, ShopType.class);
            return Result.ok(typeList);
        }
        //3、没查到，查数据库
        List<ShopType> typeList = query().orderByAsc("sort").list();
        //4、存入reids
        typeListStr = JSONUtil.toJsonStr(typeList);
        stringRedisTemplate.opsForValue().set(key,typeListStr);
        //5、返回
        return Result.ok(typeList);
    }
}
