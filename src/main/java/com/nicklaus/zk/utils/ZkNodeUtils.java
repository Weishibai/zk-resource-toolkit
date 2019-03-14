package com.nicklaus.zk.utils;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.throwIfUnchecked;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * zk node utils
 *
 * @author weishibai
 * @date 2019/03/14 2:18 PM
 */
public class ZkNodeUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkNodeUtils.class);

    private static final int DEFAULT_RETRY_TIMES = 3;

    public static String getPath(NodeCache nodeCache) {
        if (null == nodeCache) {
            return "n/a";
        }
        return nodeCache.getPath();
    }

    /**
     * @param path without namespace and must start with /
     */
    private static void setToZk(CuratorFramework client, String path, byte[] data, CreateMode createMode) {
        checkNotNull(client);
        checkNotNull(path);
        checkNotNull(data);
        checkNotNull(createMode);
        int retryTimes = 0;
        while (retryTimes++ < DEFAULT_RETRY_TIMES) {
            try {
                client.setData().forPath(path, data);
                break;
            } catch (KeeperException.NoNodeException e) {
                try {
                    client.create().creatingParentsIfNeeded()
                            .withMode(createMode)
                            .forPath(path, data);
                    break;
                } catch (KeeperException.NodeExistsException retry) {
                    continue;
                } catch (Exception e1) {
                    throwIfUnchecked(e1);
                    throw new RuntimeException(e1);
                }
            } catch (Exception e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * persistent node
     */
    public static void setToZk(CuratorFramework client, String path, byte[] data) {
        setToZk(client, path, data, CreateMode.PERSISTENT);
    }

    public static void removeFromZk(CuratorFramework client, String path, boolean recursive) {
        checkNotNull(client);
        checkNotNull(path);

        try {
            if (recursive) {
                client.delete().deletingChildrenIfNeeded().forPath(path);
            } else {
                client.delete().forPath(path);

            }
        } catch (KeeperException.NoNodeException e) {
            LOGGER.warn("{} node not exist cannot be deleted", path);
        } catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public static void removeFromZk(CuratorFramework curator, String path) {
        removeFromZk(curator, path, false);
    }


}
