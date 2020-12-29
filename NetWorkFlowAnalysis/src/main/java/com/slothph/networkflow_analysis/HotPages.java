package com.slothph.networkflow_analysis;

import com.slothph.networkflow_analysis.beans.ApacheLogEvent;
import com.slothph.networkflow_analysis.beans.PageViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author hao.peng01@hand-china.com 2020/12/29 9:36
 */
public class HotPages {

    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        //读取文件，装换成POJO
//        URL resource = HotPages.class.getResource("/apache.log");
//        DataStream<String> inputStream = env.readTextFile(resource.getPath());
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);


        DataStream<ApacheLogEvent> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(" ");
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yy:HH:mm:ss");
                    Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
                    return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                        return apacheLogEvent.getTimestamp();
                    }
                });
        dataStream.print("data");

        //分组开窗聚合
        //定义一个侧输出流标签
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late") {
        };
        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
                .filter(data -> "GET".equals(data.getMethod()))  //过滤get请求
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)  //按urL分组
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult());
        //收集同一窗口count数据，排序输出
        DataStream<String> resultStream = windowAggStream.keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));
        resultStream.print();

        env.execute("hot items analysis");
    }


    //自定义预聚合函数
    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
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

    //实现自定义的窗口函数
    public static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {

        @Override
        public void apply(String url, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(url, timeWindow.getEnd(), iterable.iterator().next()));
        }
    }

    public static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {
        private Integer topSize;
        //定义状态，保存当前所有PageViewCount到List中
//        ListState<PageViewCount> pageViewCountListState;
        MapState<String, Long> pageViewCountListState;

        public TopNHotPages(Integer topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
//            pageViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<>("page-count-list", PageViewCount.class));
            pageViewCountListState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("page-count-map", String.class, Long.class));


        }

        @Override
        public void processElement(PageViewCount pageViewCount, Context context, Collector<String> collector) throws Exception {
//            pageViewCountListState.add(pageViewCount);
            pageViewCountListState.put(pageViewCount.getUrl(), pageViewCount.getCount());
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
            //注册一个1分钟之后的定时器,用来清空状态
            context.timerService().registerProcessingTimeTimer(pageViewCount.getWindowEnd() + 60 * 1000L);


        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
//            ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(pageViewCountListState.get());
//             pageViewCounts.sort((o1, o2) -> {
//                if (o1.getCount() > o2.getCount()) {
//                    return -1;
//                } else if (o1.getCount() < o2.getCount()) {
//                    return 1;
//                } else {
//                    return 0;
//                }
//            });

            //先判断是否到了窗口关闭清理时间，如果是，直接清空状态返回
            if (timestamp == ctx.getCurrentKey() + 60 * 1000L) {
                pageViewCountListState.clear();
                return;
            }
            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountListState.entries());
            pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    if (o1.getValue() > o2.getValue()) {
                        return -1;
                    } else if (o1.getValue() < o2.getValue()) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });


            //格式化成String输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("==========");
            resultBuilder.append("窗口结束时间:").append(new Timestamp(timestamp - 1));
            resultBuilder.append("==========\n");
            //遍历列表，取top n输出
            for (int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++) {
                Map.Entry<String, Long> currentItemViewCount = pageViewCounts.get(i);
//                PageViewCount currentItemViewCount = pageViewCounts.get(i);
                resultBuilder.append("NO ").append(i + 1).append(" : ")
                        .append(" 页面URL = ").append(currentItemViewCount.getKey())
                        .append(" 浏览量 = ").append(currentItemViewCount.getValue())
                        .append("\n");
            }

            resultBuilder.append("====================================\n\n");
            //控制输出频率
            Thread.sleep(1000L);
            out.collect(resultBuilder.toString());
//            pageViewCounts.clear();
        }
    }
}



