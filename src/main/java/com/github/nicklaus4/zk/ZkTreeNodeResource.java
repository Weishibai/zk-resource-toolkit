package com.github.nicklaus4.zk;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.Thread.MIN_PRIORITY;
import static java.lang.Thread.holdsLock;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.StringUtils.removeStart;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.CONNECTION_LOST;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.CONNECTION_SUSPENDED;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.INITIALIZED;
import static org.apache.curator.utils.ThreadUtils.newThreadFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.nicklaus4.zk.model.ResourceLoader;
import com.github.nicklaus4.zk.model.ThrowableFunction;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * zk tree node resource
 *
 * @author weishibai
 * @date 2019/03/27 11:11 AM
 */
public class ZkTreeNodeResource<E> implements ResourceLoader<E>, Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkTreeNodeResource.class);

    private final Object lock = new Object();

    private final ThrowableFunction<Map<String, ChildData>, E, Exception> factory;

    private final Predicate<E> cleanup;

    private final long waitStopPeriod;

    private final BiConsumer<E, E> onResourceChange;

    private final Supplier<CuratorFramework> curatorFrameworkFactory;

    private final String path;

    @GuardedBy("lock")
    private volatile TreeCache treeCache;

    @GuardedBy("lock")
    private volatile E resource;

    @GuardedBy("lock")
    private volatile boolean closed;

    public static <E> Builder<E> newBuilder() {
        return new Builder<>();
    }

    public ZkTreeNodeResource(Builder<E> builder) {
        this.factory = builder.factory;
        this.cleanup = builder.cleanup;
        this.waitStopPeriod = builder.waitStopPeriod;
        this.path = builder.path;
        this.onResourceChange = builder.onResourceChange;
        this.curatorFrameworkFactory = builder.curatorFrameworkFactory;
    }

    private void ensureTreeCacheReady() {
        assert holdsLock(lock);
        if (null == treeCache) {
            try {
                CountDownLatch countDownLatch = new CountDownLatch(1);
                TreeCache target = TreeCache.newBuilder(curatorFrameworkFactory.get(), path) //
                        .setCacheData(true) //
                        .setExecutor(newSingleThreadExecutor(
                                newThreadFactory("TreeCache-[" + path + "]")))
                        .build();

                target.getListenable().addListener((c, event) -> {
                    if (event.getType() == INITIALIZED) {
                        countDownLatch.countDown();
                        return;
                    }

                    if (countDownLatch.getCount() > 0) {
                        LOGGER.debug("ignore event before initialized:{}=>{}", event.getType(), path);
                        return;
                    }

                    if (event.getType() == CONNECTION_SUSPENDED || event.getType() == CONNECTION_LOST) {
                        LOGGER.info("ignore event:{} for tree node:{}", event.getType(), path);
                        return;
                    }

                    E oldResource;
                    synchronized (lock) {
                        oldResource = resource;
                        resource = doFactory();
                        cleanup(resource, oldResource);
                    }
                });
                target.start();
                awaitUninterruptibly(countDownLatch);
                treeCache = target;
            } catch (Exception e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }
    }

    private E doFactory() throws Exception {
        Map<String, ChildData> map = Maps.newHashMap();
        generateFullTree(map, treeCache, path);
        return factory.apply(map);
    }

    private void generateFullTree(Map<String, ChildData> map, TreeCache cache, String rootPath) {
        Map<String, ChildData> thisMap = cache.getCurrentChildren(rootPath);
        if (thisMap != null) {
            thisMap.values().forEach(c -> map.put(removeStart(c.getPath(), path), c));
            thisMap.values().forEach(c -> generateFullTree(map, cache, c.getPath()));
        }
    }

    private void cleanup(E currentResource, E oldResource) {
        if (oldResource != null) {
            if (currentResource != oldResource) {
                new ThreadFactoryBuilder() //
                        .setNameFormat("old [" + oldResource.getClass().getSimpleName()
                                + "] cleanup thread-[%d]")
                        .setUncaughtExceptionHandler(
                                (t, e) -> LOGGER.error("fail to cleanup resource, path:{}, {}",
                                        path, oldResource.getClass().getSimpleName(), e)) //
                        .setPriority(MIN_PRIORITY) //
                        .setDaemon(true) //
                        .build() //
                        .newThread(() -> {
                            do {
                                if (waitStopPeriod > 0) {
                                    sleepUninterruptibly(waitStopPeriod, MILLISECONDS);
                                }

                                if (cleanup.test(oldResource)) {
                                    break;
                                }
                            } while (true);
                            if (onResourceChange != null) {
                                onResourceChange.accept(currentResource, oldResource);
                            }
                        }).start();
                return;
            }
        }

        if (onResourceChange != null) {
            onResourceChange.accept(currentResource, oldResource);
        }
    }

    @Override
    public E get() {
        if (closed) {
            throw new IllegalStateException("zkNode has been closed.");
        }

        if (null == resource) {
            synchronized (lock) {
                if (closed) {
                    throw new IllegalStateException("zkNode has been closed.");
                }

                if (null == resource) {
                    ensureTreeCacheReady();
                    try {
                        resource = doFactory();
                        if (onResourceChange != null) {
                            onResourceChange.accept(resource, null);
                        }
                    } catch (Exception e) {
                        throwIfUnchecked(e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return resource;
    }

    public static class Builder<E> {

        private ThrowableFunction<Map<String, ChildData>, E, Exception> factory;

        private String path;

        private Supplier<CuratorFramework> curatorFrameworkFactory;

        private Predicate<E> cleanup;

        private long waitStopPeriod;

        private BiConsumer<E, E> onResourceChange;

        @CheckReturnValue
        @Nonnull
        public Builder<E> path(String path) {
            this.path = path;
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<E> factory(ThrowableFunction<Map<String, ChildData>, E, Exception> factory) {
            this.factory = factory;
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<E> childDataFactory(ThrowableFunction<Collection<ChildData>, E, Exception> factory) {
            checkNotNull(factory);
            return factory(map -> factory.apply(map.values()));
        }

        @CheckReturnValue
        @Nonnull
        public Builder<E> onResourceChange(BiConsumer<E, E> callback) {
            this.onResourceChange = callback;
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<E> curator(Supplier<CuratorFramework> curatorFactory) {
            this.curatorFrameworkFactory = curatorFactory;
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<E> curator(CuratorFramework curator) {
            this.curatorFrameworkFactory = () -> curator;
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<E> withWaitStopPeriod(long waitStopPeriod) {
            this.waitStopPeriod = waitStopPeriod;
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<E> cleanup(Predicate<E> cleanup) {
            this.cleanup = cleanup;
            return this;
        }

        @Nonnull
        public ZkTreeNodeResource<E> build() {
            return new ZkTreeNodeResource<>(this);
        }

        private void ensure() {
            checkNotNull(factory);
            checkNotNull(curatorFrameworkFactory);

            if (onResourceChange != null) {
                BiConsumer<E, E> target = onResourceChange;
                onResourceChange = (t, u) -> {
                    try {
                        target.accept(t, u);
                    } catch (Throwable e) {
                        LOGGER.error("onResourceChange error: ", e);
                    }
                };
            }

            if (cleanup == null) {
                cleanup(t -> {
                    if (t instanceof Closeable) {
                        try {
                            ((Closeable) t).close();
                        } catch (Throwable e) {
                            throwIfUnchecked(e);
                            throw new RuntimeException(e);
                        }
                    }
                    return true;
                });
            }
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (resource != null && cleanup != null) {
                cleanup.test(resource);
            }
            if (treeCache != null) {
                treeCache.close();
            }
            closed = true;
        }
    }
}
