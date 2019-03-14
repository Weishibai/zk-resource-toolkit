package com.nicklaus.zk;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

/**
 * builder for zk resource
 *
 * @author weishibai
 * @date 2019/03/14 10:48 AM
 */
@SuppressWarnings("ALL")
public class GenericZkNodeBuilder<E> {

    private static final Logger LOGGER = getLogger(GenericZkNodeBuilder.class);

    private BiFunction<byte[], Stat, E> factory;

    private BiFunction<byte[], Stat, ListenableFuture<E>> refreshFactory;

    private Supplier<NodeCache> cacheHolder;

    private BiConsumer<E, E> onNodeChange;

    private Predicate<E> cleanup;

    private ListeningExecutorService refreshExecutor;

    private E emptyObject;

    private Runnable nodeCacheShutdown;

    private List<BiConsumer<ChildData, Throwable>> factoryFailedListeners = Lists.newArrayList();

    public BiFunction<byte[], Stat, E> buildFactory() {
        return factory;
    }

    public BiFunction<byte[], Stat, ListenableFuture<E>> refreshFactory() {
        return refreshFactory;
    }

    public Supplier<NodeCache> cacheHolder() {
        return cacheHolder;
    }

    public BiConsumer<E, E> nodeChange() {
        return onNodeChange;
    }

    public Predicate<E> cleanUp() {
        return cleanup;
    }

    public Runnable nodeCacheShutdown() {
        return nodeCacheShutdown;
    }

    public List<BiConsumer<ChildData, Throwable>> factoryFailedListeners() {
        return factoryFailedListeners;
    }

    public E emptyObject() {
        return emptyObject;
    }

    @CheckReturnValue
    @Nonnull
    public <T> GenericZkNodeBuilder<T> addFactoryFailedListener(
            @Nonnull Consumer<Throwable> listener) {
        checkNotNull(listener);
        return addFactoryFailedListener((d, t) -> listener.accept(t));
    }

    @CheckReturnValue
    @Nonnull
    public <T> GenericZkNodeBuilder<T> addFactoryFailedListener(
            @Nonnull BiConsumer<ChildData, Throwable> listener) {
        factoryFailedListeners.add(checkNotNull(listener));
        return (GenericZkNodeBuilder<T>) this;
    }

    public <T> GenericZkNodeBuilder<T> withBuildFactory(Function<byte[], T> factory) {
        return withBuildFactory((b, s) -> factory.apply(b));
    }

    public <T> GenericZkNodeBuilder<T> withBuildFactory(BiFunction<byte[], Stat, T> factory) {
        GenericZkNodeBuilder<T> thisBuilder = (GenericZkNodeBuilder<T>) this;
        thisBuilder.factory = factory;
        return thisBuilder;
    }

    public <T> GenericZkNodeBuilder<T> withRefreshableFactory(@Nullable ListeningExecutorService executor
            , Function<byte[], T> factory) {
        return withRefreshableFactory(executor, (b, s) -> factory.apply(b));
    }

    public <T> GenericZkNodeBuilder<T> withRefreshableFactory(@Nullable ListeningExecutorService executor
            , BiFunction<byte[], Stat, T> factory) {
        GenericZkNodeBuilder<T> thisBuilder = (GenericZkNodeBuilder<T>) this;
        if (null == executor) {
            thisBuilder.refreshFactory = (b, s) -> {
                try {
                    return immediateFuture(factory.apply(b, s));
                } catch (Throwable e) {
                    return immediateFailedFuture(e);
                }
            };
        } else {
            thisBuilder.refreshFactory = (b, s) -> executor.submit(() -> factory.apply(b, s));
        }
        return thisBuilder;
    }

    public <T> GenericZkNodeBuilder<T> onNodeChange(BiConsumer<? super T, ? super T> callback) {
        GenericZkNodeBuilder<T> thisBuilder = (GenericZkNodeBuilder<T>) this;
        thisBuilder.onNodeChange = (BiConsumer<T, T>) callback;
        return thisBuilder;
    }

    @CheckReturnValue
    @Nonnull
    public GenericZkNodeBuilder<E> withNodeFactory(Supplier<NodeCache> cacheHolder) {
        this.cacheHolder = cacheHolder;
        return this;
    }

    public GenericZkNodeBuilder<E> withNodeFactory(String path, CuratorFramework curator) {
        return withNodeFactory(path, () -> curator);
    }

    @CheckReturnValue
    @Nonnull
    public GenericZkNodeBuilder<E> withNodeFactory(String path, Supplier<CuratorFramework> curatorFactory) {
        this.cacheHolder = () -> {
            CuratorFramework thisClient = curatorFactory.get();
            if (thisClient.getState() != CuratorFrameworkState.STARTED) {
                thisClient.start();
            }
            NodeCache buildingCache = new NodeCache(thisClient, path);
            try {
                buildingCache.start();
                // not safety check but do it better. due to IE breaks in rebuild.
                if (Thread.currentThread().isInterrupted()) {
                    Thread.interrupted();
                }
                buildingCache.rebuild();
                this.nodeCacheShutdown = () -> {
                    try {
                        buildingCache.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                };
                return buildingCache;
            } catch (Throwable e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        };
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public <T> GenericZkNodeBuilder<T> withCleanupPredicate(Predicate<? super T> cleanup) {
        GenericZkNodeBuilder<T> thisBuilder = (GenericZkNodeBuilder<T>) this;
        thisBuilder.cleanup = (Predicate<T>) cleanup;
        return thisBuilder;
    }

    @CheckReturnValue
    @Nonnull
    public <T> GenericZkNodeBuilder<T> withCleanupConsumer(Consumer<? super T> cleanup) {
        GenericZkNodeBuilder<T> thisBuilder = (GenericZkNodeBuilder<T>) this;
        thisBuilder.cleanup = t -> {
            try {
                cleanup.accept(t);
                return true;
            } catch (Throwable e) {
                LOGGER.error("fail to close zk path: {}", t, e);
                return false;
            }
        };
        return thisBuilder;
    }

    @CheckReturnValue
    @Nonnull
    public <T> GenericZkNodeBuilder<T> withEmptyObject(T emptyObject) {
        GenericZkNodeBuilder<T> thisBuilder = (GenericZkNodeBuilder<T>) this;
        thisBuilder.emptyObject = emptyObject;
        return thisBuilder;
    }

    @Nonnull
    public <T> ZkNodeResource<T> build() {
        precondition();
        return new ZkNodeResource(this);
    }

    /**
     * make sure required params all set
     */
    private void precondition() {
        checkNotNull(factory);
        checkNotNull(cacheHolder);

        if (refreshFactory == null) {
            if (refreshExecutor != null) {
                refreshFactory = (bs, stat) -> refreshExecutor
                        .submit(() -> factory.apply(bs, stat));
            } else {
                refreshFactory = (bs, stat) -> {
                    try {
                        return immediateFuture(factory.apply(bs, stat));
                    } catch (Throwable t) {
                        return immediateFailedFuture(t);
                    }
                };
            }
        }

        if (onNodeChange != null) {
            BiConsumer<E, E> temp = onNodeChange;
            onNodeChange = (t, u) -> {
                try {
                    temp.accept(t, u);
                } catch (Throwable e) {
                    LOGGER.error("zk node change error: ", e);
                }
            };
        }

        if (cleanup == null) {
            withCleanupConsumer(t -> {
                if (t instanceof Closeable) {
                    try {
                        ((Closeable) t).close();
                    } catch (Throwable e) {
                        throw propagate(e);
                    }
                }
            });
        }
    }


}
