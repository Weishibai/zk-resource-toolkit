package com.nicklaus.zk.factory;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.nicklaus.zk.model.ZkClientKey;

/**
 * zk client factory to get CuratorFramework
 *
 * //todo 暂时取消重连监听等功能
 *
 * @author weishibai
 * @date 2019/03/14 10:30 AM
 */
public class ZkClientCachedFactory {

    private static final int DEFAULT_SESSION_TIMEOUT_MS = 60 * 1000;

    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 50 * 1000;

    private static final int DEFAULT_RETRY_TIMES = 3;

    private static final int DEFAULT_RETRY_INTERVAL = 3000; //MS

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkClientCachedFactory.class);

    private static final ConcurrentMap<ZkClientKey, CuratorFramework> clients = Maps.newConcurrentMap();

    /**
     *
     * @param zkConnectStr like ip:port
     * @param namespace /root
     */
    public static CuratorFramework get(@Nonnull String zkConnectStr, @Nullable String namespace) {
        requireNonNull(zkConnectStr);
        ZkClientKey key = new ZkClientKey(zkConnectStr, namespace);
        return clients.computeIfAbsent(key, k -> {
            final CuratorFramework curator = CuratorFrameworkFactory.builder()
                    .connectString(k.getZkConnectStr())
                    .namespace(k.getNamespace())
                    .sessionTimeoutMs(DEFAULT_SESSION_TIMEOUT_MS)
                    .connectionTimeoutMs(DEFAULT_CONNECTION_TIMEOUT_MS)
                    .retryPolicy(new RetryNTimes(DEFAULT_RETRY_TIMES, DEFAULT_RETRY_INTERVAL))
                    .build();
            curator.start();

            LOGGER.info("create zk client addr {} namespace {}", key.getZkConnectStr(), key.getNamespace());
            return curator;
        });
    }

    public static CuratorFramework get(@Nonnull String zkConnectStr) {
        return get(zkConnectStr, null);
    }
}
