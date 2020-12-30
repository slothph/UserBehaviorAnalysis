package com.slothph.networkflow_analysis;

import com.slothph.networkflow_analysis.beans.PageViewCount;
import com.slothph.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.HashSet;

/**
 * @author hao.peng01@hand-china.com 2020/12/30 14:35
 */
public class UniqueVisitor {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取文件，装换成POJO
        URL resource = UniqueVisitor.class.getResource("/UserBehavior.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());


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
        //开窗统计uv值
        SingleOutputStreamOperator<PageViewCount> uvStream = dataStream.filter((data -> "pv".equals(data.getBehavior())))
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountResult());
        uvStream.print();
        env.execute("uv count job");
    }

    //实现自定义全窗口函数
    public static class UvCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        @Override
        public void apply(TimeWindow timeWindow, Iterable<UserBehavior> iterable, Collector<PageViewCount> collector) throws Exception {
            //定义一个Set结构，保存窗口中的所有userID，自动去重
            HashSet<Long> uidSet = new HashSet<>();
            for (UserBehavior ub : iterable) {
                uidSet.add(ub.getUserId());
            }
            collector.collect(new PageViewCount("uv", timeWindow.getEnd(), (long) uidSet.size()));

        }
    }
}
