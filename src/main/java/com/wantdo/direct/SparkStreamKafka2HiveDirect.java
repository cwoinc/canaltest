//package com.wantdo.direct;
//
//import kafka.common.TopicAndPartition;
//import kafka.message.MessageAndMetadata;
//import kafka.serializer.StringDecoder;
//import kafka.utils.ZKGroupTopicDirs;
//import kafka.utils.ZkUtils;
//import org.I0Itec.zkclient.ZkClient;
//import org.I0Itec.zkclient.ZkConnection;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.hive.HiveContext;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.HasOffsetRanges;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//import org.apache.spark.streaming.kafka.OffsetRange;
//import org.apache.zookeeper.ZooDefs;
//
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.atomic.AtomicReference;
//
///**
// * 采用Direct 方式拉取kafka数据到hive表,可以用hiveSql写，也可以直接将数据写入对应的hive目录，然后执行任意刷新语句
// * ALTER TABLE xxx ADD IF NOT EXISTS PARTITION (yue='2018-05',ri='2018-05-20')
// * offset手动提交到zookeeper
// */
//public class SparkStreamKafka2HiveDirect {
//
//    public static void main(String[] args) {
//        String topic = "";
//        String group = "";
//        SparkConf conf = new SparkConf().setAppName("待保存队列到hive");
//        //削峰,在任务积压时，会减少每秒的拉取量
//        conf.set("spark.streaming.backpressure.enabled", "true");
//        // maxRetries默认就是1 接受数据相关的一共就只有两个配置
//        conf.set("spark.streaming.kafka.maxRetries", "1");
//        //每秒最多拉取partition * 2 的数据
//        conf.set("spark.streaming.kafka.maxRatePerPartition", "2");
//
//        JavaSparkContext jsc = new JavaSparkContext(conf);
//        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));
//        // 如果在这里初始化hivecontext,在下面的算子内使用hivecontext会报一个空指针异常,原因貌似是用的时候Hivecontext未初始化成功(请知道的大佬普及一下)
//        // HiveContext hiveContext = new HiveContext(jsc);
//        // kafka 参数
//        HashMap<String, String> kafkaParams = new HashMap<>();
//        kafkaParams.put("metadata.broker.list", "");
//        kafkaParams.put("group.id", group);
//        // kafkaParams.put("auto.offset.reset", "smallest");
//        Set<String> topicSet = new HashSet<>();
//        topicSet.add(topic);
//        // 赋值操作不是线程安全的。若想不用锁来实现，可以用AtomicReference<V>这个类，实现对象引用的原子更新
//        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
//        // 读取zookeeper中消费组的偏移量
//        ZKGroupTopicDirs zgt = new ZKGroupTopicDirs(group, topic);
//        final String zkTopicPath = zgt.consumerOffsetDir();
//        // System.out.println(zkTopicPath);
//        // 会写在zookeeper根目录下consumers下！！！
//        ZkClient zkClient = new ZkClient("");
//        int countChildren = zkClient.countChildren(zkTopicPath);
//
//        Map<TopicAndPartition, Long> fromOffsets = new HashMap<>();
//
//        if (countChildren > 0) {
//            for (int i = 0; i < countChildren; i++) {
//                String path = zkTopicPath + "/" + i;
//                String offset = zkClient.readData(path);
//                TopicAndPartition topicAndPartition = new TopicAndPartition("", i);
//                fromOffsets.put(topicAndPartition, Long.parseLong(offset));
//            }
//            /**
//             * createDirectStream(JavaStreamingContext jssc, java.lang.Class<K> keyClass,
//             * java.lang.Class<V> valueClass, java.lang.Class<KD> keyDecoderClass,
//             * java.lang.Class<VD> valueDecoderClass, java.lang.Class<R> recordClass,
//             * java.util.Map<java.lang.String,java.lang.String> kafkaParams,
//             * java.util.Map<kafka.common.TopicAndPartition,java.lang.Long> fromOffsets,
//             * Function<kafka.message.MessageAndMetadata<K,V>,R> messageHandler) Create an
//             * input stream that directly pulls messages from Kafka Brokers without using
//             * any receiver.
//             */
//            //幸亏java8支持lambda表达式呀，要不然写惯了Scala的人简直没法活了~~~
//            KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, String.class, kafkaParams, fromOffsets, MessageAndMetadata::message).foreachRDD(rdd -> {
//                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//                offsetRanges.set(offsets);
//                // 逻辑处理
//                HiveContext hiveContext = new HiveContext(jsc);
//                try {
//                    //1-采用hivecontext执行"insert into table" 插入数据到hive
//                    //2-将DF以Hive的存储格式存到Hive目录下
//                    //更新zookeeper
//                    ZkClient zkClient1 = new ZkClient("");
//                    OffsetRange[] offsets1 = offsetRanges.get();
//                    if (null != offsets1) {
//                        for (OffsetRange o : offsets1) {
//                            String zkPath = zkTopicPath + "/" + o.partition();
//                            // System.out.println(zkPath + o.untilOffset());
//                            new ZkUtils(zkClient1, new ZkConnection(""), false).updatePersistentPath(zkPath, o.untilOffset() + "", ZooDefs.Ids.OPEN_ACL_UNSAFE);
//                        }
//                    }
//                    zkClient.close();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            });
//        } else {
//            KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicSet).foreachRDD(rdd -> {
//                if (!rdd.isEmpty()) {
//                    OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//                    offsetRanges.set(offsets);
//
//                    HiveContext hiveContext = new HiveContext(jsc);
//                    try {
//                        //处理逻辑代码
//                        // 更新zookeeper
//                        ZkClient zkClient1 = new ZkClient("");
//                        OffsetRange[] offsets1 = offsetRanges.get();
//                        if (null != offsets1) {
//                            for (OffsetRange o : offsets1) {
//                                String zkPath = zkTopicPath + "/" + o.partition();
//                                new ZkUtils(zkClient1, new ZkConnection(""), false).updatePersistentPath(zkPath, o.untilOffset() + "", ZooDefs.Ids.OPEN_ACL_UNSAFE);
//                            }
//                        }
//                        zkClient.close();
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//            });
//        }
//        jssc.start();
//        try {
//            jssc.awaitTermination();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//}
