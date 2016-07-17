package com.lushuai;

/**分布式锁服务接口，该接口定义了如下功能
 *
 * 参考：　http://wujiu.iteye.com/blog/2223715
 *
 *  <ul>
 *     <li> tryLock 一直等待锁</li>
 *     <li> tryLock 等待一段时间，如果超时则会调用回调方法expire()</li>
 *   </ul>
 *
 * Created by lushuai on 16-7-17.
 */
public interface DistributedLockService {
    /**
     * 试图获取分布式锁，如果返回true则表示获取了锁
     *
     * @param callback 回调接口
     */
    public void tryLock(CallBack callback);

    /**
     * 视图获取分布式锁，如果在指定timeout时间后容然未能够获取到锁则返回
     *
     * @param callback
     * @param timeout
     */
    public void tryLock(CallBack callback, long timeout);

    /**
     * 回调处理接口
     *
     * @author zhangwei_david
     * @version $Id: DistributedLockService.java, v 0.1 2015年7月2日 上午10:59:22 zhangwei_david Exp $
     */
    public interface CallBack {
        /**
         * 获取分布式锁后回调方法
         */
        public void locked();

        /**
         * 获取分布式锁超时回调方法
         */
        public void expire();
    }

}
