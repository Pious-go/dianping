package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author luocheng
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;


    public Result sendCode(String phone, HttpSession session) {
        //校验手机号是否合法
        if(RegexUtils.isPhoneInvalid(phone)){
            //不合法
            return Result.fail("手机号码格式不正确！");
        }
        //如果合法，生成验证码并保存在session中
        String code = RandomUtil.randomNumbers(6);
        //session.setAttribute("code",code);
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //发送验证码给用户（这里没有使用短信服务或者邮箱服务，直接打印在控制台了）
        log.info("发送的验证码：{}",code);
        return Result.ok();
    }

    //优化：改造发验证码，增加黑名单功能和邮箱发验证码服务
//    @Override
//    public Result sendCode(String phone, HttpSession session, HttpServletRequest request) throws MessagingException {
//        // 1.校验手机号
//        if (RegexUtils.isPhoneInvalid(phone)) {
//            // 2.如果不符合，返回错误信息
//            return Result.fail("手机号格式错误！");
//        }
//        // 3. 获取客户端IP地址
//        String clientIpAddr = request.getRemoteAddr();
//
//        // 4.如果存在锁，直接返回失败
//        String phoneGetCodeLock = stringRedisTemplate.opsForValue().get(GET_CODE_LOCK + phone);
//        if (phoneGetCodeLock != null) {
//            return Result.fail("获取验证码过快，请稍后重试！");
//        }
//
//        // 5.检查两个黑名单中的次数
//        String blacklistPhoneCount = stringRedisTemplate.opsForValue().get(GET_CODE_BLACKLIST_PHONE + phone);
//        int getCodePhoneCount = (blacklistPhoneCount != null) ? Integer.parseInt(blacklistPhoneCount) : 0;
//        if (getCodePhoneCount >= 400) {
//            return Result.fail("获取验证码次数过多，您的手机号已被限制！");
//        }
//
//        String blacklistIpAddrCount = stringRedisTemplate.opsForValue().get(GET_CODE_BLACKLIST_IP_ADDR + clientIpAddr);
//        int getCodeIpAddrCount = (blacklistIpAddrCount != null) ? Integer.parseInt(blacklistIpAddrCount) : 0;
//        if (getCodeIpAddrCount >= 300) {
//            return Result.fail("获取验证码次数过多，您的IP已被限制！");
//        }
//
//        // 6.更新锁和黑名单
//        stringRedisTemplate.opsForValue().set(GET_CODE_BLACKLIST_PHONE + phone, String.valueOf(getCodePhoneCount + 1), REFRESH_BLACKLIST_TTL, TimeUnit.HOURS);
//        stringRedisTemplate.opsForValue().set(GET_CODE_BLACKLIST_IP_ADDR + clientIpAddr, String.valueOf(getCodeIpAddrCount + 1), REFRESH_BLACKLIST_TTL, TimeUnit.HOURS);
//        stringRedisTemplate.opsForValue().set(GET_CODE_LOCK + phone, "1", GET_CODE_LOCK_TTL, TimeUnit.MINUTES);
//
//        // 7.校验通过，生成验证码
////        String code = RandomUtil.randomNumbers(6);
////        String code = "123123"; //为了方便测试，改成固定值
//        String code = MailUtils.achieveCode();
//        // 8.保存验证码到 redis，并上锁
//        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);
//
//        // 9.发送验证码
//        log.info("发送短信验证码成功，验证码：{}", code);
//        //发送验证码
//        MailUtils.sendtoMail(phone, code);
//        // 10.返回ok
//        return Result.ok();
//    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //得到客户端传来的手机号和验证码
        String phone = loginForm.getPhone();
        String code = loginForm.getCode();
        //校验手机号和验证码
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号码格式不正确！");
        }
        // String sessionCode = (String)session.getAttribute("code");
        //获取redis中的验证码
        String redisCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        if (!redisCode.equals(code) || code == null) {
            return Result.fail("验证码有错误！");
        }
        //正确的话根据手机号查询用户
//        LambdaQueryWrapper<User> queryWrapper = new LambdaQueryWrapper<User>();
//        queryWrapper.eq(User::getPhone, phone);
//        User user = userService.getOne(queryWrapper);
        User user = query().eq("phone", phone).one();
        if (user == null) {
            //不存在则创建新用户，并保存到数据库
            user = createUserWithPhone(phone);
        }
        //保存到session
        //session.setAttribute("user",user);
        //UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        //session.setAttribute("user", userDTO);
        //保存用户信息到Redis中
        //随机生成token，作为登录令牌
        String token = UUID.randomUUID().toString();
        //将UserDto对象转为HashMap存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
//        HashMap<String, String > userMap = new HashMap<>();
//        userMap.put("icon", userDTO.getIcon());
//        userMap.put("id", String.valueOf(userDTO.getId()));
//        userMap.put("nickName", userDTO.getNickName());
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create().setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));

        //存储
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        //设置token有效期为30分钟
        stringRedisTemplate.expire(tokenKey, 30, TimeUnit.MINUTES);
        //登陆成功则删除验证码信息
        stringRedisTemplate.delete(LOGIN_CODE_KEY + phone);
        //返回token
        return Result.ok(token);
    }

    //登录功能优化，优化点：使用分离存储策略避免Redis中的用户信息冗余（同一用户多次登录，生成多份token）
//    @Override
//    public Result login(LoginFormDTO loginForm, HttpSession session) {
//        String phone = loginForm.getPhone();
//        String code = loginForm.getCode();
//        // 1. 校验手机号格式
//        if (RegexUtils.isPhoneInvalid(phone)) {
//            return Result.fail("手机号码格式不正确！");
//        }
//        // 2. 从Redis获取验证码
//        String redisCodeKey = LOGIN_CODE_KEY + phone;
//        String redisCode = stringRedisTemplate.opsForValue().get(redisCodeKey);
//        // 3. 验证码校验（包含null检查）
//        if (code == null || !code.equals(redisCode)) {
//            // 3. 不一致，报错
//            return Result.fail("验证码有误！");
//        }
//        // 4. 验证码一致，根据手机号查询用户
//        User user = query().eq("phone", phone).one();
//        if (user == null) {
//            user = createUserWithPhone(phone);  // 创建新用户
//        }
//        // 5. 生成Token（每次登录都不同）
//        String token = UUID.randomUUID().toString();
//        // 6. 核心优化：分离存储
//        // 6.1 存储Token->用户ID映射（热数据）
//        String tokenKey = LOGIN_TOKEN_KEY + token;
//        stringRedisTemplate.opsForValue().set(
//                tokenKey,
//                user.getId().toString(),
//                LOGIN_TOKEN_TTL,
//                TimeUnit.MINUTES
//        );
//        // 6.2 存储用户信息（温数据）
//        String userKey = LOGIN_USER_KEY + user.getId();
//        if (Boolean.FALSE.equals(stringRedisTemplate.hasKey(userKey))) {
//            // 用户信息不存在时才存储
//            UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
//            Map<String, Object> userMap = BeanUtil.beanToMap(
//                    userDTO,
//                    new HashMap<>(),
//                    CopyOptions.create()
//                            .setIgnoreNullValue(true)
//                            .setFieldValueEditor((fieldName, fieldValue) ->
//                                    fieldValue != null ? fieldValue.toString() : null
//                            )
//            );
//            stringRedisTemplate.opsForHash().putAll(userKey, userMap);
//            stringRedisTemplate.expire(userKey, LOGIN_USER_TTL, TimeUnit.HOURS);
//        } else {
//            // 存在则刷新TTL
//            stringRedisTemplate.expire(userKey, LOGIN_USER_TTL, TimeUnit.HOURS);
//        }
//
//        // 7. 清理验证码
//        stringRedisTemplate.delete(redisCodeKey);
//
//        // 8. 返回Token
//        return Result.ok(token);
//    }




    @Override
    public Result sign() {
        //1. 获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2. 获取日期
        LocalDateTime now = LocalDateTime.now();
        //3. 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //4. 获取今天是当月第几天(1~31)
        int dayOfMonth = now.getDayOfMonth();
        //5. 写入Redis  BITSET key offset 1
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth - 1,true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        //1. 获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2. 获取日期
        LocalDateTime now = LocalDateTime.now();
        //3. 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //4. 获取今天是当月第几天(1~31)
        int dayOfMonth = now.getDayOfMonth();
        //5. 获取截止至今日的签到记录  BITFIELD key GET uDay 0
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create()
                .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));
        if (result == null || result.isEmpty()) {
            return Result.ok(0);
        }
        //循环遍历
        int count = 0;
        Long num = result.get(0);
        while (true) {
            if ((num & 1) == 0) {
                break;
            } else
                count++;
            //数字右移，抛弃最后一位
            num >>>= 1;
        }
        return Result.ok(count);
    }

    private User createUserWithPhone(String phone) {
        //创建用户
        User user = new User();
        //设置手机号
        user.setPhone(phone);
        //设置昵称(默认名)，一个固定前缀+随机字符串
        user.setNickName("user_" + RandomUtil.randomString(8));
        //保存到数据库
        save(user);
        return user;
    }
}
