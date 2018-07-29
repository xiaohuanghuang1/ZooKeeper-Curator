package com.imooc.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CuratorOperator1
{
    private static final Logger logger = LoggerFactory.getLogger(CuratorOperator1.class);

    public CuratorFramework client = null;
    public static final String zkServerPath = "192.168.118.130:2181,192.168.118.130:2182,192.168.118.130:2183";


    public CuratorOperator1()
    {
        RetryPolicy retryPolicy = new RetryNTimes(3,3000);


        //namesapce类似eclipse的工作空间 zk会生成namespace的节点
        client = CuratorFrameworkFactory.builder()
                .connectString(zkServerPath)
                .sessionTimeoutMs(10000).retryPolicy(retryPolicy)
                .namespace("workspace").build();

        client.start();

    }

    public void closeZKClient()
    {
        if (client != null)
        {
            this.client.close();
        }
    }

    public static void main(String[] args) throws Exception
    {
        CuratorOperator1 cto = new CuratorOperator1();

        boolean isStarted = cto.client.isStarted();
       logger.info("当前客户端状态:{}",isStarted);

        String nodePath = "/super/imooc";
       //创建节点
//        byte[] data = "superme".getBytes();
//        cto.client.create().creatingParentContainersIfNeeded()
//                .withMode(CreateMode.PERSISTENT)
//                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
//                .forPath(nodePath,data);

        //修改数据
//        byte[] newData = "batman".getBytes();
//        cto.client.setData().withVersion(7)
//                .forPath(nodePath,newData);

        //删除数据
//        cto.client.delete().guaranteed()
//                .deletingChildrenIfNeeded()
//                .withVersion(1).forPath(nodePath);

        //只监听一次
//        cto.client.getData()
//                .usingWatcher(new MyCuratorWatcher())
//                .forPath(nodePath);


        //监听数据节点的变更，会触发事件 持续监听
//        final NodeCache nodeCache = new NodeCache(cto.client,nodePath);
        //为true时会将node数据获取到并缓存到本地
//        nodeCache.start(true);
//        if (nodeCache.getCurrentData() != null)
//        {
//            logger.info("节点初始化数据为:{}",new String(nodeCache.getCurrentData().getData()));
//        }
//        else
//        {
//            logger.info("节点初始化数据为空....");
//        }
//        nodeCache.getListenable().addListener(new NodeCacheListener()
//        {
//            @Override
//            public void nodeChanged() throws Exception
//            {
//                String data = new String(nodeCache.getCurrentData().getData());
//                logger.info("节点路径:{},数据:{}",nodeCache.getCurrentData().getPath(),data);
//            }
//        });


        //为子节点添加watcher
        final PathChildrenCache childrenCache = new PathChildrenCache(cto.client,nodePath,true);
        /**
         * StartMode 初始化方式
         * POST_INITIALIZED_EVENT 异步初始化，初始化之后会触发事件
         * NORMAL:异步初始化
         * BUILD_INITIAL_CACHE 同步初始化 会将节点数据打印出来
         */
        childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
//        List<ChildData> childDataList = childrenCache.getCurrentData();
//        logger.info("=====当前子节点列表=====");
//        childDataList.stream().forEach(data->{
//            String childData = new String(data.getData());
//            logger.info(childData);
//        });

        //为子节点添加持续监听事件
        childrenCache.getListenable().addListener((curatorFramework, pathChildrenCacheEvent) ->
        {
            if (pathChildrenCacheEvent.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED))
            {
                logger.info("子节点初始化OK。。。。。。");
            }
            else if (pathChildrenCacheEvent.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED))
            {
                logger.info("添加子节点:{}",pathChildrenCacheEvent.getData().getPath());
                logger.info("子节点数据:{}",new String(pathChildrenCacheEvent.getData().getData()));
            }
            else if (pathChildrenCacheEvent.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED))
            {
                logger.info("删除子节点:{}",pathChildrenCacheEvent.getData().getPath());
            }
            else if (pathChildrenCacheEvent.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED))
            {
                logger.info("修改子节点:{}",pathChildrenCacheEvent.getData().getPath());
                logger.info("子节点数据:{}",new String(pathChildrenCacheEvent.getData().getData()));
            }
        });


        Thread.sleep(300000);

        cto.closeZKClient();
        isStarted = cto.client.isStarted();
        logger.info("当前客户端状态:{}",isStarted);
    }


}
