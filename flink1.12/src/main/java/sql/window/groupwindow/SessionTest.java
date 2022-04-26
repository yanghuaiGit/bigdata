/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sql.window.groupwindow;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class SessionTest {
    @Test
    public void processtime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> waterSensorStream = env.socketTextStream("127.0.0.1", 9999)
                .map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return Tuple3.of(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });


        // 2. 创建表: 将流转换成动态表. 表的字段名从pojo的属性名自动抽取
        Table table = tableEnv.fromDataStream(waterSensorStream
                , $("id")
                , $("ts")
                , $("vc")
                , $("pt").proctime());

        //开滚动窗口计算wordCount
        Table select = table.window(Session.withGap(lit(5).seconds()).on($("pt")).as("sw"))
                .groupBy($("id"), $("sw"))
                .select($("id"), $("id").count());

        //结果转为流进行输出 追加流，一个窗口只计算一次
        tableEnv.toAppendStream(select, Row.class).print();
        env.execute();
    }

    @Test
    public void eventTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //读取数据转换为JavaBean 并提取时间戳生成waterMark
        WatermarkStrategy<Tuple3<String, Long, Integer>> objectWatermarkStrategy = WatermarkStrategy
                .<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, Integer>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Integer> waterSensor, long l) {
                        return waterSensor.f1 * 1000L;
                    }
                });

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> waterSensorStream = env.socketTextStream("127.0.0.1", 9999)
                .map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return Tuple3.of(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                }).assignTimestampsAndWatermarks(objectWatermarkStrategy);

        // 2. 创建表: 将流转换成动态表. 表的字段名从pojo的属性名自动抽取,并指定事件时间字段
        Table table = tableEnv.fromDataStream(waterSensorStream
                , $("id")
                ,$("ts")
                ,$("vc")
                ,$("rt").rowtime());

        //开滚动窗口计算wordCount
        //数据本身和waterMark的时间差距  waterMark是延迟2s 所以就是5+2=7s  输入1，还要输入1+7=8 才能输出
        Table select = table.window(Session.withGap(lit(5).seconds()).on($("rt")).as("sw"))
                .groupBy($("id"), $("sw"))
                .select($("id"), $("id").count());

        //结果转为流进行输出 追加流，一个窗口只计算一次
        tableEnv.toAppendStream(select, Row.class).print();
        env.execute();
    }
}
