package com.lushuai.baseapi;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 参考：http://wujiu.iteye.com/blog/2207872
 * Created by lushuai on 16-7-17.
 */
public class ZKDemo {

    private static ZooKeeper authZK = null;

    private static String    path   = "/zk-demo";

    /**
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        sync();
        TimeUnit.SECONDS.sleep(300);
        createZKWithSessionIdAndSessionPasswd();
        authCreate();
    }

    /**
     * 指定权限
     *
     * @throws Exception
     */
    private static void authCreate() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        authZK = new ZooKeeper("localhost:2181", 5000, new Watcher() {

            public void process(WatchedEvent event) {
                System.out.println("监听器，监听到的事件时：" + event);
                if (Event.KeeperState.SyncConnected == event.getState()) {
                    //如果客户端已经建立连接闭锁减一
                    System.out.println("建立连接");
                    latch.countDown();
                }
            }
        });
        // 等待连接建立
        latch.await();
        // 增加权限
        authZK.addAuthInfo("digest", "foo:true".getBytes());
        // 判断path 是否已经存在
        if (authZK.exists(path, true) == null) {
            authZK.create(path, "init".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        }
        String str = authZK.create(path + "/auth", "test".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL,
                CreateMode.EPHEMERAL);
        System.out.println(str);

    }

    private static void createZKWithSessionIdAndSessionPasswd() throws Exception {
        // 初始化一个闭锁
        final CountDownLatch latch = new CountDownLatch(1);

        /**
         * 创建一个ZooKeeper对象，localhost:2181是zk服务器的主机名和端口号
         *  50000  sessionTimeout session超时时间
         *  Watcher 默认的WatchedEvent事件处理器
         */
        ZooKeeper originalZk = new ZooKeeper("localhost:2181", 50000, new Watcher() {

            public void process(WatchedEvent event) {
                /**
                 * 如果zk客户端和服务器已经建立连接，闭锁减一
                 */
                if (Event.KeeperState.SyncConnected == event.getState()) {
                    latch.countDown();
                }
            }
        });
        // 等待闭锁为0，即一直等待zk客户端和服务器建立连接
        latch.await();

        // 通过sessionId,和sessionPasswd创建一个复用的ZK客户端
        ZooKeeper zkCopy = new ZooKeeper(path, 50000, new Watcher() {

            public void process(WatchedEvent event) {

            }
        }, originalZk.getSessionId(), originalZk.getSessionPasswd());

        System.out.println("复用zk" + zkCopy + " original zk=" + originalZk);

    }

    /**
     *
     *
     * @return
     * @throws Exception
     */
    private static ZooKeeper sync() throws Exception {
        // 初始化一个闭锁
        final CountDownLatch latch = new CountDownLatch(1);

        /**
         * 创建一个ZooKeeper对象，localhost:2181是zk服务器的主机名和端口号
         *  50000  sessionTimeout session超时时间
         *  Watcher 默认的WatchedEvent事件处理器
         */
        ZooKeeper zk = new ZooKeeper("localhost:2181", 50000, new Watcher() {

            public void process(WatchedEvent event) {
                System.out.println("默认监听器，KeeperStat:" + event.getState() + ", EventType:"
                        + event.getType() + ", path:" + event.getPath());
                /**
                 * 如果zk客户端和服务器已经建立连接，闭锁减一
                 */
                if (Event.KeeperState.SyncConnected == event.getState()) {
                    latch.countDown();
                }
            }
        });
        // 等待闭锁为0，即一直等待zk客户端和服务器建立连接
        latch.await();

        zk.getData(path, true, new AsyncCallback.DataCallback() {

            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                System.out.println("rc=" + rc + ",path=" + path + "data=" + data + " Stat=" + stat
                        + ", data=" + (data == null ? "null" : new String(data)));
            }
        }, null);

        // 创建一个临时节点,非安全的权限
        zk.create(path + "/linshi", "ephemeral".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, new AsyncCallback.StringCallback() {

                    public void processResult(int rc, String path, Object ctx, String name) {
                        if (rc == 0) {
                            System.out.println("成功创建一个临时节点");
                        }
                    }
                }, null);

        // 更新path节点数据
        zk.setData(path, String.valueOf(new Date().getTime()).getBytes(), -1, new AsyncCallback.StatCallback() {

            public void processResult(int rc, String path, Object ctx, Stat stat) {
                if (rc == 0) {
                    System.out.println("成功更新节点数据， rc=0, path=" + path + ", stat=" + stat);
                }
            }
        }, null);

        /**
         * 如果该节点不存在则创建持久化的节点
         * ZK的节点有三位，持久化节点（PERSISTENT）,临时节点（EPHEMERAL），顺序节点（SEQUENTIAL）
         * 具体的组合有 /**

         PERSISTENT,
         //持久化序列节点
         PERSISTENT_SEQUENTIAL,

         EPHEMERAL ,
         //临时序列节点
         EPHEMERAL_SEQUENTIAL
         *
         */
        // 创建一个持久化节点，如果该节点已经存在则不需要再次创建
        if (zk.exists(path, true) == null) {
            zk.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        // 删除持久化序列节点
        try {
            zk.delete(path + "/seq ", 1, new AsyncCallback.VoidCallback() {

                public void processResult(int rc, String path, Object ctx) {
                    System.out.println("删除" + ZKDemo.path + "/seq 的结果是：rc=" + rc + " path=" + path
                            + ",context=" + ctx);
                }
            }, null);
        } catch (Exception e) {
            // 示例代码创建新的持久节点前，如果以前存在则删除，如果不存在则或抛出一个NoNodeException
        }
        // 创建一个持久化序列节点
        String seqPath = zk
                .create(path + "/seq ", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //读取子节点列表，并注册一个监听器
        zk.getChildren(path, new Watcher() {

            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        });
        //更新节点数据
        zk.setData(seqPath, "seqDemo".getBytes(), -1);

        //创建临时节点
        zk.create(path + "/test", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        // 删除节点
        zk.delete(path + "/test", 0);

        // 异步创建临时序列节点
        zk.create(path + "/test", "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL, new AsyncCallback.StringCallback() {

                    public void processResult(int rc, String path, Object ctx, String name) {
                        System.out.println("创建临时节点的结果是：result code=" + rc + ", path=" + path
                                + " context=" + ctx + ", name=" + name);
                    }
                }, "Test Context");

        TimeUnit.SECONDS.sleep(5);

        // 更新数据
        if (zk.exists(path + "/test", true) != null) {
            zk.setData(path + "/test", "testData".getBytes(), -1);
        }
        zk.create(path + "/test2", "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        // 使用默认监听器，获取子节点
        List<String> znodes = zk.getChildren(path, true);
        // 获取所有节点下的数据
        Stat stat = new Stat();
        for (String str : znodes) {
            byte[] data = zk.getData(path + "/" + str, true, stat);

            System.out.println("获取节点：" + str + " 的数据是："
                    + (data == null ? "null" : new String(data)));
        }
        return zk;
    }
}
