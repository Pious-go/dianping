package com.hmdp.controller;


import com.hmdp.dto.OrderPaymentDTO;
import com.hmdp.dto.Result;
import com.hmdp.service.IVoucherOrderService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author luocheng
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {
    @Resource
    private IVoucherOrderService voucherOrderService;

    //普通券
    @PostMapping("/common/{id}")
    public Result commonlVoucher(@PathVariable("id") Long voucherId) {
        int buyNumber = 1;
        return voucherOrderService.commonVoucher(voucherId, buyNumber);
    }

    //限购券
    @PostMapping("/limit/{id}")
    public Result limitlVoucher(@PathVariable("id") Long voucherId) {
        int buyNumber = 1;
        return voucherOrderService.limitVoucher(voucherId, buyNumber);
    }

    //秒杀券
    @PostMapping("seckill/{id}")
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        int buyNumber = 1;
        return voucherOrderService.seckillVoucher(voucherId,buyNumber);
    }

    //支付
    @PostMapping("/payment")
    public Result payment(@RequestBody OrderPaymentDTO orderPaymentDTO) {
        return voucherOrderService.payment(orderPaymentDTO);
    }


}
