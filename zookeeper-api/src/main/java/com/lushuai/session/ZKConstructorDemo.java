package com.lushuai.session;


import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

/**
 *创建一个最基本的ZooKeeper会话示例
 */
public class ZKConstructorDemo implements Watcher {

    private static CountDownLatch connectedSemphore = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        //创建Zookeeper会话实例
        ZooKeeper zookeeper = new ZooKeeper("127.0.0.1:2181", 5000, new ZKConstructorDemo());
        // 输出当前会话的状态
        System.out.println("zk客户端的状态是：" + zookeeper.getState());
        System.out.println("zk 客户端的sessionId=" + zookeeper.getSessionId() + ",  sessionPasswd是："
                + new String(zookeeper.getSessionPasswd()));
        try {
            // 当前闭锁在为0之前一直等待，除非线程中断
            connectedSemphore.await();
        } catch (Exception e) {
            System.out.println("Zookeeper session established");
        }
    }

    /**
     * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
     */
    public void process(WatchedEvent event) {
        System.out.println("Receive watched event:" + event);
        //如果客户端已经处于连接状态闭锁减去1
        if (Event.KeeperState.SyncConnected == event.getState()) {
            connectedSemphore.countDown();
        }
    }
}