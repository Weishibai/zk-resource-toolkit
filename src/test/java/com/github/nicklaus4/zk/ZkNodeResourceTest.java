package com.github.nicklaus4.zk;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.github.nicklaus4.zk.factory.ZkClientCachedFactory;

/**
 * zk node resource test
 *
 * @author weishibai
 * @date 2019/03/14 2:31 PM
 */
@RunWith(MockitoJUnitRunner.class)
public class ZkNodeResourceTest {


    @Before
    public void init() {

    }

    private String buildString(byte[] data) {
        return new String(data);
    }

//    private TestModel buildJson(byte[] data) {
//        try {
//            ObjectMapper mapper = new ObjectMapper();
//            return mapper.readValue(data, TestModel.class);
//        } catch (Exception e) {
//            return null;
//        }
//    }

//    @Test
    public void testTree() {
        final ZkTreeNodeResource<List<String>> node = ZkTreeNodeResource.<List<String>>newBuilder()
                .childDataFactory(this::buildChilds)
                .path("/test/tree")
                .curator(ZkClientCachedFactory.get("localhost:2181", "nicklaus"))
                .build();

        System.out.println(node.get());
    }

    private List<String> buildChilds(Collection<ChildData> children) {
        return children.stream()
                .map(child -> new String(child.getData()))
                .collect(Collectors.toList());
    }

//    @Test
    public void testGet() {

        String path = "/test";

        final ZkNodeResource<String> holder = ZkNodeResource.<String>newBuilder()
                .withNodeFactory(path, () -> ZkClientCachedFactory.get("localhost:2181", "nicklaus"))
                .withBuildFactory(this::buildString)
                .build();

        System.out.println(holder.get());
        holder.closeQuietly();
    }

//    @Test
    public void testJson() {
//        final CuratorFramework curator = ZkClientCachedFactory.get("localhost:2181", "nicklaus");
//        TestModel model = new TestModel();
//        model.setId("123");
//        model.setName("hhhhhh");
//        ObjectMapper mapper = new ObjectMapper();
//        try {
//            ZkNodeUtils.setToZk(curator, "/test/json", mapper.writeValueAsBytes(model));
//        } catch (JsonProcessingException e) {
//
//        }

//        String path = "/test/json";
//        final ZkNodeResource<TestModel> holder = ZkNodeResource.<TestModel>newBuilder()
//                .withNodeFactory(path, () -> ZkClientCachedFactory.get("localhost:2181", "nicklaus"))
//                .withBuildFactory(this::buildJson)
//                .build();
//
//        System.out.println(holder.get());
//        holder.closeQuietly();
    }

//    @Test
    public void testOnChange() {
//        String path = "/test/json";
//
//        final ZkNodeResource<TestModel> holder = ZkNodeResource.<TestModel>newBuilder()
//                .withNodeFactory(path, () -> ZkClientCachedFactory.get("localhost:2181", "nicklaus"))
//                .withBuildFactory(this::buildJson)
//                .onNodeChange((current, old) -> {
//                    System.out.println("current " + current + " -- " + "old " + old);
//                })
//                .build();
//
//        System.out.println(holder.get());
//
//        try {
//            TimeUnit.SECONDS.sleep(30);
//            System.out.println(holder.get());
//        } catch (InterruptedException e) {
//
//        }
//        holder.closeQuietly();
    }




}
