package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author luocheng
 * @since 2021-12-22
 */
public interface IShopTypeService extends IService<ShopType> {
    Result queryShopType();

    // Result queryList();
}
