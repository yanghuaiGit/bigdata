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

package table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.apache.flink.table.api.Expressions.$;

public class StreamToTable {
    @Test
    public void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorStream = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2. todo 好像不行哎 创建表: 将流转换成动态表. 表的字段名从pojo的属性名自动抽取
        Table table = tableEnv.fromDataStream(waterSensorStream);
        // 3. 对动态表进行查询
        Table resultTable = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"));
        // 4. 把动态表转换成流
        DataStream<Row> resultStream = tableEnv.toAppendStream(resultTable, Row.class);
        resultStream.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void test1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple3> waterSensorStream = env.fromElements(Tuple3.of("sensor_1", 1000L, 10),
                Tuple3.of("sensor_1", 2000L, 20),
                Tuple3.of("sensor_2", 3000L, 30),
                Tuple3.of("sensor_1", 4000L, 40),
                Tuple3.of("sensor_1", 5000L, 50),
                Tuple3.of("sensor_2", 6000L, 60));

        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2. 创建表: 将流转换成动态表. 表的字段名从pojo的属性名自动抽取
        Table table = tableEnv.fromDataStream(waterSensorStream, "id,name,vc");
        // 3. 对动态表进行查询
        Table resultTable = table
                .where($("id").isEqual("sensor_1"))
                .select($("name"));
        // 4. 把动态表转换成流
        DataStream<Row> resultStream = tableEnv.toAppendStream(resultTable, Row.class);
        resultStream.print();
        env.execute();
    }


    @Test
    public void testStream() throws Exception {
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
        Table table = tableEnv.fromDataStream(waterSensorStream, $("id"),$("name"),$("vc"));
        // 3. 对动态表进行查询
        Table resultTable = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"),$("name"),$("vc"));
        // 4. 把动态表转换成流
        DataStream<Row> resultStream = tableEnv.toAppendStream(resultTable, Row.class);
        resultStream.print();
        env.execute();
    }
}
