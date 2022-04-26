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

package sql.time;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;
import table.WaterSensor;

import javax.xml.bind.Element;
import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class Time {

    @Test
    public void testProcessTimeStreamToTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> waterSensorStream = env.socketTextStream("127.0.0.1", 9999)
                .map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return Tuple3.of(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2. 创建表: 将流转换成动态表. 表的字段名从pojo的属性名自动抽取
        Table table = tableEnv.fromDataStream(waterSensorStream
                , $("id")
                ,$("ts")
                ,$("vc")
                ,$("pt").proctime());

        table.printSchema();
    }


    @Test
    public void testEventTimeStreamToTable() throws Exception {
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

        table.printSchema();
        // 3. 对动态表进行查询
        Table resultTable = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"),$("ts"),$("vc"),$("rt"));
        // 4. 把动态表转换成流
        DataStream<Row> resultStream = tableEnv.toAppendStream(resultTable, Row.class);
        resultStream.print();
        env.execute();
    }
}
