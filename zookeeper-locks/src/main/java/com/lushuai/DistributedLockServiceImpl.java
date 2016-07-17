package com.lushuai;

import org.apache.zookeeper.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 分布式锁服务实现类
 * Created by lushuai on 16-7-17.
 */
public class DistributedLockServiceImpl implements DistributedLockService {
    private static final String ROOT           = "/zk-demo";

    /**锁的临时节点**/
    private static final String LOCK           = "lock";

    /**排它锁节点**/
    private static final String EXCLUSIVE_LOCK = ROOT + "/" + LOCK;

    private int                 sessionTimeout = 3000;


    public void tryLock(final CallBack callback, long timeout) {
        try {
            final long expireTime = timeout > 0 ? System.currentTimeMillis() + timeout : -1;
            final ZooKeeper zk = getZooKeeper();
            //向根节点注册一个子节点变化监听器
            List<String> nodes = zk.getChildren(ROOT, new Watcher() {

                public void process(WatchedEvent event) {
                    // 排它锁已经被释放，则视图获取锁
                    if (event.getState() == Event.KeeperState.SyncConnected
                            && event.getType() == Event.EventType.NodeChildrenChanged) {
                        doLock(zk, callback, expireTime);
                    }
                }
            });
            // 没有人获取锁则视图获取锁
            if (!nodes.contains(LOCK)) {
                doLock(zk, callback, expireTime);
            }

        } catch (Exception e) {

        }
    }

    /**
     *
     */
    public void tryLock(final CallBack callback) {
        tryLock(callback, -1);
    }

    /**
     * 具体执行分布式锁，如果拥有分布式锁则执行callback回调，然后释放锁
     *
     * @param zk
     * @param callback
     * @param expireTime 过期时间
     */
    private void doLock(ZooKeeper zk, CallBack callback, long expireTime) {
        try {
            if (expireTime > 0 && System.currentTimeMillis() > expireTime) {
                callback.expire();
                return;
            }
            String path = zk
                    .create(EXCLUSIVE_LOCK, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println(path);
            if (path != null) {
                callback.locked();
                zk.delete(EXCLUSIVE_LOCK, -1);
            }
        } catch (Exception e) {

        } finally {
            try {
                zk.close();
            } catch (InterruptedException e) {

            }
        }
    }

    /**
     * 获取ZooKeeper
     *
     * @return
     * @throws Exception
     */
    private ZooKeeper getZooKeeper() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper("localhost:2181", sessionTimeout, new Watcher() {

            public void process(WatchedEvent event) {
                if (Event.KeeperState.SyncConnected == event.getState()) {
                    //如果客户端已经建立连接闭锁减一
                    latch.countDown();
                }
            }
        });
        // 等待连接建立
        latch.await();
        return zk;
    }

    /**
     * Getter method for property <tt>sessionTimeout</tt>.
     *
     * @return property value of sessionTimeout
     */
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Setter method for property <tt>sessionTimeout</tt>.
     *
     * @param sessionTimeout value to be assigned to property sessionTimeout
     */
    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }
}
