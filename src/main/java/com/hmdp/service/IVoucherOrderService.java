package com.hmdp.service;

import com.hmdp.dto.OrderPaymentDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author luocheng
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId,int buyNumber);

    Result commonVoucher(Long voucherId, int buyNumber);

    Result limitVoucher(Long voucherId, int buyNumber);

    void createVoucherOrder(VoucherOrder voucherOrder);

    Result payment(OrderPaymentDTO orderPaymentDTO);
}
