package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.OrderPaymentDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.CommonVoucher;
import com.hmdp.entity.Event;
import com.hmdp.entity.LimitVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.event.KafkaOrderProducer;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ICommonVoucherService;
import com.hmdp.service.ILimitVoucherService;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.KafkaConstants.TOPIC_CREATE_ORDER;
import static com.hmdp.utils.KafkaConstants.TOPIC_SAVE_ORDER_FAILED;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_KEY;
import static com.hmdp.utils.SystemConstants.MAX_BUY_LIMIT;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author luocheng
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private KafkaOrderProducer kafkaOrderProducer;
    @Resource
    private ICommonVoucherService commonVoucherService;
    @Resource
    private ILimitVoucherService limitVoucherService;

    /**
     * 加载 判断秒杀券库存是否充足 并且 判断用户是否已下单 的Lua脚本
     */
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
//
//    /**
//     * VoucherOrderServiceImpl类的代理对象
//     * 将代理对象的作用域进行提升，方面子线程取用
//     */
//    private IVoucherOrderService proxy;
//
//    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
//
//    /**
//     * 当前类初始化完毕就立马执行该方法
//     */
//    @PostConstruct
//    private void init() {
//        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
//    }
//
//    /**
//     * 线程任务: 不断从消息队列中获取订单
//     */
//    private class VoucherOrderHandler implements Runnable {
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
//                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                            Consumer.from("g1", "c1"),
//                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
//                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
//                    );
//                    // 2.判断订单信息是否为空
//                    if (list == null || list.isEmpty()) {
//                        // 如果为null，说明没有消息，继续下一次循环
//                        continue;
//                    }
//                    // 解析数据
//                    MapRecord<String, Object, Object> record = list.get(0);
//                    Map<Object, Object> value = record.getValue();
//                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
//                    // 3.创建订单
//                    createVoucherOrder(voucherOrder);
//                    // 4.确认消息 SACK stream.orders g1 id
//                    stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
//                } catch (Exception e) {
//                    log.error("处理订单异常", e);
//                    // 处理异常消息
//                    handlePendingList();
//                }
//            }
//        }
//
//        private void handlePendingList() {
//            while (true) {
//                try {
//                    // 1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 0
//                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                            Consumer.from("g1", "c1"),
//                            StreamReadOptions.empty().count(1),
//                            StreamOffset.create("stream.orders", ReadOffset.from("0"))
//                    );
//                    // 2.判断订单信息是否为空
//                    if (list == null || list.isEmpty()) {
//                        // 如果为null，说明没有异常消息，结束循环
//                        break;
//                    }
//                    // 解析数据
//                    MapRecord<String, Object, Object> record = list.get(0);
//                    Map<Object, Object> value = record.getValue();
//                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
//                    // 3.创建订单
//                    createVoucherOrder(voucherOrder);
//                    // 4.确认消息 XACK
//                    stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
//                } catch (Exception e) {
//                    log.error("处理订单异常", e);
//                }
//            }
//        }
//    }

    /*private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while (true){
                try {
                    // 1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2.创建订单
                    createVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }*/

    /**
     * 创建订单
     *
     * @param voucherOrder
     */
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if (!isLock) {
            // 获取锁失败，直接返回失败或者重试
            log.error("不允许重复下单！");
            return;
        }
        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                log.error("不允许重复下单！");
                return;
            }
            // 6.扣减库存
//            boolean success = seckillVoucherService.update()
//                    .setSql("stock = stock - 1") // set stock = stock - 1
//                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
//                    .update();
            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - " + voucherOrder.getBuyNumber()) // set stock = stock - buynumber
                    .eq("voucher_id", voucherId)
                    .gt("stock", voucherOrder.getBuyNumber()) // where id = ? and stock > buynumber
                    .update();
            if (!success) {
                // 扣减失败
                log.error("库存不足！");
                return;
            }
            // 7.创建订单
            //save(voucherOrder);
            voucherOrder.setCreateTime(LocalDateTime.now());
            voucherOrder.setUpdateTime(LocalDateTime.now());
            if (!save(voucherOrder)) {
                log.info("保存订单失败");
                throw new Exception("保存订单失败");
            }
        }catch (Exception e) {
            //如果保存订单过程中发生异常，则构造一个事件对象 Event，设置相关业务数据和失败主题，
            // 并通过 Kafka 发布该事件以进行后续处理（如异步补偿或日志追踪）
                Map<String, Object> data = new HashMap<>();
                data.put("voucherId", voucherId);
                data.put("buyNumber", voucherOrder.getBuyNumber());
                Event event = new Event()
                        .setTopic(TOPIC_SAVE_ORDER_FAILED)
                        .setUserId(userId)
                        .setEntityId(voucherOrder.getId())
                        .setData(data);
                kafkaOrderProducer.publishEvent(event);

        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }


    /**
     * 抢购秒杀券
     *
     * @param voucherId
     * @return
     */
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        Long userId = UserHolder.getUser().getId();
//        long orderId = redisIdWorker.nextId("order");
//        // 1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString(), String.valueOf(orderId)
//        );
//        int r = result.intValue();
//        // 2.判断结果是否为0
//        if (r != 0) {
//            // 2.1.不为0 ，代表没有购买资格
//            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
//        }
//        // 3.返回订单id
//        //主线程获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        return Result.ok(orderId);
//    }

    /*@Override Java 自带的阻塞队列 BlockingQueue 实现消息队列
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        int r = result.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 2.2.为0 ，有购买资格，把下单信息保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        // 2.3.订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 2.4.用户id
        voucherOrder.setUserId(userId);
        // 2.5.代金券id
        voucherOrder.setVoucherId(voucherId);
        // 2.6.放入阻塞队列
        orderTasks.add(voucherOrder);

        // 3.返回订单id
        return Result.ok(orderId);
    }*/

    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 2.判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀尚未开始！");
        }
        // 3.判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀已经结束！");
        }
        // 4.判断库存是否充足
        if (voucher.getStock() < 1) {
            // 库存不足
            return Result.fail("库存不足！");
        }
        return createVoucherOrder(voucherId);
    }

    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();
        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if(!isLock){
            // 获取锁失败，直接返回失败或者重试
            return Result.fail("不允许重复下单！");
        }
        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买过一次！");
            }
            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }
            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1.订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2.用户id
            voucherOrder.setUserId(userId);
            // 7.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);
            // 7.返回订单id
            return Result.ok(orderId);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }*/

    /*@Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();

        // 创建锁对象
        SimpleRedisLock redisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock(1200);
        // 判断
        if(!isLock){
            // 获取锁失败，直接返回失败或者重试
            return Result.fail("不允许重复下单！");
        }
        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买过一次！");
            }
            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }
            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1.订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2.用户id
            voucherOrder.setUserId(userId);
            // 7.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);
            // 7.返回订单id
            return Result.ok(orderId);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }*/

    /*@Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();
        synchronized (userId.toString().intern()) {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买过一次！");
            }
            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }
            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1.订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2.用户id
            voucherOrder.setUserId(userId);
            // 7.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);
            // 7.返回订单id
            return Result.ok(orderId);
        }
    }*/


    @Override
    public Result commonVoucher(Long voucherId, int buyNumber) {
        //1.查询优惠券
        CommonVoucher voucher = commonVoucherService.getById(voucherId);
        //2.判断库存是否充足
        if(voucher.getStock() < buyNumber){
            // 库存不足
            return Result.fail("库存不足！");
        }
        //3.乐观锁扣减库存
        boolean success = commonVoucherService.update()
                .setSql("stock = stock - " + buyNumber) // set stock = stock - 1
                .eq("voucher_id", voucherId)
                .gt("stock", buyNumber - 1)
                .update();//  where id = ? and stock >= buyNumber
        //扣减库存
        if(!success){
            // 扣减失败
            return Result.fail("库存不足！");
        }
        //4.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 4.1.订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 4.2.用户id
        voucherOrder.setUserId(UserHolder.getUser().getId());
        // 4.3.代金券id
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);

        return Result.ok(orderId);
    }

    @Override
    @Transactional
    public Result limitVoucher(Long voucherId, int buyNumber) {
        Long userId = UserHolder.getUser().getId();
        // 1.查询优惠券
        LimitVoucher limitVoucher = limitVoucherService.getById(voucherId);
        Integer limitCount = limitVoucher.getLimitCount();
        //2. 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:limitvoucher" + voucherId + userId);
        try {
            // 3.判断库存是否充足
            if(limitVoucher.getStock() < buyNumber){
                //库存不足
                return Result.fail("库存不足！");
            }
            // 4.判断是否限购
            // 执行查询
            List<VoucherOrder> orderList = this.list(new LambdaQueryWrapper<VoucherOrder>()
                    .eq(VoucherOrder::getUserId, userId)
                    .eq(VoucherOrder::getVoucherId, voucherId));
            // 计算购买数量总和
            int totalBuyNumber = orderList.stream().mapToInt(VoucherOrder::getBuyNumber).sum();

            if (totalBuyNumber + buyNumber > limitCount) {
                return Result.fail("超过最大购买限制!");
            }

            // 5.尝试获取锁，最多等待10s
            boolean isLock = false;
            isLock = redisLock.tryLock(10L, TimeUnit.SECONDS);
            // 判断
            if (!isLock) {
                //获取锁失败，直接返回失败或者重试
                log.error("获取锁失败！");
                return Result.fail("同一时间下单人数过多，请稍后重试");
            }
            //6.乐观锁扣减库存
            boolean success = limitVoucherService.update()
                    .setSql("stock = stock - " + buyNumber) // set stock = stock - 1
                    .eq("voucher_id", voucherId)
                    .gt("stock", buyNumber - 1)
                    .update();//where id = ? and stock >= buyNumber
            if(!success){
                // 扣减失败
                return Result.fail("库存不足！");
            }
            //7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder().setId(redisIdWorker.nextId("order"))
                    .setVoucherId(voucherId)
                    .setUserId(userId)
                    .setCreateTime(LocalDateTime.now())
                    .setUpdateTime(LocalDateTime.now())
                    .setStatus(1)
                    .setBuyNumber(buyNumber);
            save(voucherOrder);
            //8.返回结果
            return Result.ok(voucherOrder);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            redisLock.unlock();
        }
    }


    @Override
    public Result seckillVoucher(Long voucherId, int buyNumber) {
        Long userId = UserHolder.getUser().getId();
        long currentTime = LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long orderId = redisIdWorker.nextId("order");
        try {
            // 1.执行lua脚本
            Long result = stringRedisTemplate.execute(
                    SECKILL_SCRIPT,
                    Collections.emptyList(),
                    voucherId.toString(),
                    userId.toString(),
                    String.valueOf(orderId),
                    String.valueOf(currentTime),
                    String.valueOf(buyNumber),
                    String.valueOf(MAX_BUY_LIMIT)
            );

            switch (result.intValue()) {
                case 0:
                    // 2.秒杀成功，发送消息到kafka
                    sendOrderMsgToKafka(orderId, voucherId, userId, buyNumber);
                    // 返回订单id
                    return Result.ok(orderId);
                case 1:
                    // TODO 获取锁，读取 mysql 数据存放到 Redis 中，然后递归调用本函数
                    return Result.fail("redis缺少数据");
                case 2:
                    return Result.fail("秒杀尚未开始");
                case 3:
                    return Result.fail("秒杀已经结束");
                case 4:
                    return Result.fail("库存不足");
                case 5:
                    return Result.fail("超过最大购买限制");
                default:
                    return Result.fail("未知错误");
            }
        } catch (Exception e) {
            log.error("处理订单异常", e);
            return Result.fail("未知错误");
        }
    }

    //将订单信息发送到Kafka消息队列
    public void sendOrderMsgToKafka(long orderId, Long voucherId, Long userId, int buyNumber) {
        Map<String, Object> data = new HashMap<>();//创建包含优惠券ID和购买数量的数据Map
        data.put("voucherId", voucherId);
        data.put("buyNumber", buyNumber);
        Event event = new Event()
                .setTopic(TOPIC_CREATE_ORDER)
                .setUserId(userId)
                .setEntityId(orderId)
                .setData(data);
        kafkaOrderProducer.publishEvent(event);
    }


    @Override
    public Result payment(OrderPaymentDTO orderPaymentDTO) {
        // 参数校验
        if (orderPaymentDTO == null || orderPaymentDTO.getOrderId() == null) {
            return Result.fail("参数错误");
        }

        Long orderId = orderPaymentDTO.getOrderId();
        Long userId = UserHolder.getUser().getId();


        int retryCount = 0;
        final int MAX_RETRY = 3;

        while (retryCount++ < MAX_RETRY) {
            // 1. 查询 Redis 判断订单是否正在被处理（消息队列中未消费）
            boolean isRedisExist = stringRedisTemplate.opsForSet().isMember(SECKILL_ORDER_KEY, orderId);

            if (Boolean.TRUE.equals(isRedisExist)) {
                // 订单正在处理中，等待重试
                try {
                    Thread.sleep(1000); // 每次等待1秒
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Result.fail("支付中断");
                }
                continue; // 继续重试
            }

            // 2. Redis 中不存在，查询数据库是否存在该订单
            VoucherOrder voucherOrder = this.getOne(new LambdaQueryWrapper<VoucherOrder>()
                    .eq(VoucherOrder::getUserId, userId)
                    .eq(VoucherOrder::getId, orderId));

            if (voucherOrder != null) {
                // 3. 数据库中存在订单，进入支付流程
                // TODO: 执行支付逻辑，如扣款、更新状态等
                return Result.ok("可以进入支付流程");
            } else {
                // 4. Redis 和 DB 都不存在订单
                return Result.fail("订单不存在");
            }
        }
        // 达到最大重试次数仍未完成
        return Result.fail("订单处理超时，请稍后再试");
    }








}
