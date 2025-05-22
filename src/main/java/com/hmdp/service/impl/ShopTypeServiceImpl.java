package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_LIST_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author luocheng
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
//
//    @Override
//    public Result queryList() {
//        // 先从Redis中查，这里的常量值是固定前缀 + 店铺id
//        List<String> shopTypes =
//                stringRedisTemplate.opsForList().range(CACHE_SHOP_TYPE_KEY, 0, -1);
//        // 如果不为空（查询到了），则转为ShopType类型直接返回
//        if (!shopTypes.isEmpty()) {
//            List<ShopType> tmp = shopTypes.stream().map(type -> JSONUtil.toBean(type, ShopType.class))
//                    .collect(Collectors.toList());
//            return Result.ok(tmp);
//        }
//        // 否则去数据库中查
//        List<ShopType> tmp = query().orderByAsc("sort").list();
//        if (tmp == null) {
//            return Result.fail("店铺类型不存在！！");
//        }
//        // 查到了转为json字符串，存入redis
//        shopTypes = tmp.stream().map(type -> JSONUtil.toJsonStr(type))
//                .collect(Collectors.toList());
//        stringRedisTemplate.opsForList().leftPushAll(CACHE_SHOP_TYPE_KEY, shopTypes);
//        // 最终把查询到的商户分类信息返回给前端
//        return Result.ok(tmp);
//    }


    @Override
    public Result queryShopType() {
        // 1.从 Redis 中查询商铺缓存
        String shopTypeJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE_LIST_KEY);

        // 2.判断 Redis 中是否存在数据
        if (StrUtil.isNotBlank(shopTypeJson)) {
            // 2.1.存在，则返回
            List<ShopType> shopTypes = JSONUtil.toList(shopTypeJson, ShopType.class);
            return Result.ok(shopTypes);
        }
        // 2.2.Redis 中不存在，则从数据库中查询
        List<ShopType> shopTypes = query().orderByAsc("sort").list();

        // 3.判断数据库中是否存在
        if (shopTypes == null) {
            // 3.1.数据库中也不存在，则返回 false
            return Result.fail("分类不存在！");
        }
        // 3.2.数据库中存在，则将查询到的信息存入 Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_TYPE_LIST_KEY, JSONUtil.toJsonStr(shopTypes));
        // 3.3返回
        return Result.ok(shopTypes);
    }
}

