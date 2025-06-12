package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 36000L;

    //10到14行是优化后的内容
    //private static final String LOGIN_TOKEN_KEY = "login:token:";  // token->userID映射
    //private static final String LOGIN_USER_KEY = "login:user:";    // 用户信息存储
    // 过期时间配置
    //private static final Long LOGIN_TOKEN_TTL = 30L;     // Token有效期30分钟
    //private static final Long LOGIN_USER_TTL = 24L;      // 用户信息有效期24小时


    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";
    public static final Long CACHE_SHOP_TYPE_TTL = 30L;
    public static final String CACHE_SHOP_TYPE_KEY = "cache:shoptype:";
    public static final String CACHE_SHOP_TYPE_LIST_KEY = "cache:shopType:";


    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    public static final String SECKILL_ORDER_KEY = "seckill:order";
    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";


    public static final String GET_CODE_LOCK = "getCodeLock:phone:";
    public static final String GET_CODE_BLACKLIST_PHONE = "getCodeBlacklist:phone:";
    public static final String GET_CODE_BLACKLIST_IP_ADDR = "getCodeBlacklist:ipAddr:";
    public static final String GET_CODE_BLACKLIST_MAC_ADDR = "getCodeBlacklist:macAddr:";
    public static final Long GET_CODE_LOCK_TTL = 1L;
    public static final Long REFRESH_BLACKLIST_TTL = 24L;
}
