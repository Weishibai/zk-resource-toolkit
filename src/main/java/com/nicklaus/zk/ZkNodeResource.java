package com.nicklaus.zk;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.nicklaus.zk.utils.ZkNodeUtils.getPath;
import static java.lang.Thread.MIN_PRIORITY;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.nicklaus.zk.model.ZkNodeState;

/**
 * zk node resource
 *
 * @author weishibai
 * @date 2019/03/14 11:34 AM
 */
public class ZkNodeResource<E> implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkNodeResource.class);

    private BiFunction<byte[], Stat, E> factory;

    private BiFunction<byte[], Stat, ListenableFuture<E>> refreshFactory;

    private Supplier<NodeCache> cacheHolder;

    private BiConsumer<E, E> onNodeChange;

    private Predicate<E> cleanup;

    private E emptyObject;

    private Runnable nodeCacheShutdown;

    private BiConsumer<ChildData, Throwable> factoryFailedListener;

    private final Object lock = new Object();

    private volatile E resource;

    private volatile boolean closed = false;

    private volatile boolean hasNodeListener = false;

    private volatile Runnable nodeCacheRemoveListener;

    private volatile ZkNodeState nodeState = ZkNodeState.UNKNOWN;


    public ZkNodeResource(GenericZkNodeBuilder<E> builder) {
        this.factory = builder.buildFactory();
        this.refreshFactory = builder.refreshFactory();
        this.cacheHolder = builder.cacheHolder();
        this.cleanup = builder.cleanUp();
        this.nodeCacheShutdown = builder.nodeCacheShutdown();
        this.onNodeChange = builder.nodeChange();
        this.emptyObject = builder.emptyObject();
        this.factoryFailedListener = (child, e) -> {
            final List<BiConsumer<ChildData, Throwable>> listeners = builder.factoryFailedListeners();
            listeners.forEach(listener -> {
                try {
                    listener.accept(child, e);
                } catch (Throwable e1) {
                    LOGGER.error("", e1);
                }
            });
        };
    }

    @CheckReturnValue
    @Nonnull
    public static <E> GenericZkNodeBuilder<E> newBuilder() {
        return new GenericZkNodeBuilder<>();
    }

    public E get() {
        if (closed) {
            throw new IllegalStateException("zkNode has been closed.");
        }

        if (nodeState == ZkNodeState.NON_EXIST) {
            return emptyObject;
        }

        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("zkNode has been closed.");
            }

            if (null == resource) {
                final NodeCache nodeCache = cacheHolder.get();
                addNodeListener(nodeCache);

                final ChildData currentData = nodeCache.getCurrentData();
                if (null == currentData || null == currentData.getData()) {
                    nodeState = ZkNodeState.NON_EXIST;
                    LOGGER.warn("current path {} is empty.", getPath(nodeCache));
                    return emptyObject;
                }

                nodeState = ZkNodeState.EXIST;
                try {
                    resource = factory.apply(currentData.getData(), currentData.getStat());
                    if (null != onNodeChange) {
                        onNodeChange.accept(resource, emptyObject);
                    }
                } catch (Exception e) {
                    factoryFailedListener.accept(currentData, e);
                    throwIfUnchecked(e);
                    throw new RuntimeException(e);
                }
            }
        }
        return resource;
    }

    public boolean hasClosed() {
        return closed;
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            if (nodeCacheShutdown != null) {
                nodeCacheShutdown.run();
            }
            if (nodeCacheRemoveListener != null) {
                nodeCacheRemoveListener.run();
            }
            if (resource != null && resource != emptyObject && cleanup != null) {
                cleanup.test(resource);
            }
            closed = true;
        }
    }

    public void closeQuietly() {
        if (!hasClosed()) {
            try {
                close();
            } catch (Exception e) {
                LOGGER.error("close error: ", e);
            }
        }
    }

    private void addNodeListener(NodeCache cache) {
        if (!hasNodeListener) {
            NodeCacheListener nodeCacheListener = () -> {
                E oldResource;
                synchronized (lock) {
                    ChildData data = cache.getCurrentData();
                    oldResource = resource;
                    if (data != null && data.getData() != null) {
                        nodeState = ZkNodeState.EXIST;
                        ListenableFuture<E> future = refreshFactory.apply(data.getData(), data.getStat());
                        addCallback(future, new FutureCallback<E>() {

                            @Override
                            public void onSuccess(@Nullable E result) {
                                resource = result;
                                cleanup(resource, oldResource, cache);
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                factoryFailedListener.accept(data, t);
                                LOGGER.error("", t);
                            }
                        }, directExecutor());
                    } else {
                        nodeState = ZkNodeState.NON_EXIST;
                        resource = null;
                        cleanup(resource, oldResource, cache);
                    }
                }
            };
            cache.getListenable().addListener(nodeCacheListener);
            nodeCacheRemoveListener = () -> cache.getListenable().removeListener(nodeCacheListener);
            hasNodeListener = true;
        }
    }

    private void cleanup(E currentResource, E oldResource, NodeCache nodeCache) {
        if (oldResource != null && oldResource != emptyObject) {
            if (currentResource != oldResource) {
                new ThreadFactoryBuilder() //
                        .setNameFormat("old [" + oldResource.getClass().getSimpleName()
                                + "] cleanup thread-[%d]")
                        .setUncaughtExceptionHandler(
                                (t, e) -> LOGGER.error("fail to cleanup resource, path:{}, {}",
                                        getPath(nodeCache), oldResource.getClass().getSimpleName(), e)) //
                        .setPriority(MIN_PRIORITY) //
                        .setDaemon(true) //
                        .build() //
                        .newThread(() -> {
                            cleanup.test(oldResource);
                            if (onNodeChange != null) {
                                onNodeChange.accept(currentResource, oldResource);
                            }
                        }).start();
                return;

            }
        }

        if (onNodeChange != null) {
            onNodeChange.accept(currentResource, oldResource);
        }
    }


}
