package com.slothph.hotitems_analysis;

import com.slothph.hotitems_analysis.beans.ItemViewCount;
import com.slothph.hotitems_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author hao.peng01@hand-china.com 2020/12/28 9:44
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2.读取数据，创建DataStream
//        DataStream<String> inputStream = env.readTextFile("D:\\WorkSpace\\GitDepository\\Code\\UserBehaviorAnalysis\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");


        Properties properties = new Properties();
//        properties.setProperty("zookeeper.connect", "172.23.16.112:2181");
        properties.setProperty("bootstrap.servers", "172.23.16.112:6667");
        properties.setProperty(" group.id", "consumer");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");


        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("hotitems", new SimpleStringSchema(), properties));


        //3.转换为POJO，分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp() * 1000L;
                    }
                });
        //4.分组开窗聚合，得到每个窗口内各个商品的count值
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior())// 过滤pv行为
                ).keyBy("itemId")                       // 按照商品ID分组
                .timeWindow(Time.hours(1), Time.minutes(5))    // 设置滑动窗口进行统计
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());

        //5.收集统一窗口的所有商品count数据，排序输出top n
        DataStream<String> resultStream = windowAggStream
                .keyBy("windowEnd")                     // 按照窗口分组，收集当前窗口内的商品count数据
                .process(new TopNHotItems(5));          // 自定义处理流程

        resultStream.print();
        env.execute("hot items analysis");
    }

    //实现自定义增量聚合函数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {


        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    //自定义全窗口函数
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            Long itemId = tuple.getField(0);
            Long windowEnd = timeWindow.getEnd();
            Long count = iterable.iterator().next();
            collector.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    //实现自定义KeyedProcessFunction
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
        //定义属性，top n的size
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        //定义列表状态
        //定义列表状态，保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<>("item-view-count-list", ItemViewCount.class));

        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            //每来一条数据，存入List中，并注册定时器
            itemViewCountListState.add(itemViewCount);
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {


            //定时器触发，当前已收集到所有数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            itemViewCounts.sort((o1, o2) -> o2.getCount().intValue() - o1.getCount().intValue());

            //将排名信息格式化成String，方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("==========");
            resultBuilder.append("窗口结束时间:").append(new Timestamp(timestamp - 1));
            resultBuilder.append("==========\n");
            //遍历列表，取top n输出
            for (int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++) {
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i + 1).append(" : ")
                        .append("商品ID = ").append(currentItemViewCount.getItemId())
                        .append("热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }

            resultBuilder.append("====================================\n\n");
            //控制输出频率
            Thread.sleep(1000L);
            out.collect(resultBuilder.toString());
        }
    }
}
