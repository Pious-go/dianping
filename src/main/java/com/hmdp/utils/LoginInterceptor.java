package com.hmdp.utils;

import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class LoginInterceptor implements HandlerInterceptor {

    /*
    * 前置拦截器，用于判断用户是否登录
    * */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //获取session
//        HttpSession session = request.getSession();
//        //User user = (User) session.getAttribute("user");
//        UserDTO user = (UserDTO) session.getAttribute("user");
//        //判断用户是否存在
//        if(user == null){
//            //不存在，拦截
//            response.setStatus(401);
//            return false;
//        }
//        //存在就保存用户信息到ThreadLocal，UserHolder是提供好了的工具类
//        UserHolder.saveUser(user);
//        //放行
//        return true;

        //判断用户是否存在
        if (UserHolder.getUser()==null){
            //不存在则拦截
            response.setStatus(401);
            return false;
        }
        //存在则放行
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}
