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

package sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

public class KafkaSql {


    @Test
    public void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> waterSensorStream = env.socketTextStream("127.0.0.1", 9999)
                .map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return Tuple3.of(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });


        Table table = tableEnv.fromDataStream(waterSensorStream, "id,ts,vc");
        Table result = tableEnv.sqlQuery("select id,ts,vc from " + table + " where id ='001'");
        tableEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }


    @Test
    public void agg() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> waterSensorStream = env.socketTextStream("127.0.0.1", 9999)
                .map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return Tuple3.of(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });


        tableEnv.createTemporaryView("sensor", waterSensorStream, "id,ts,vc");


        Table result = tableEnv.sqlQuery("select id,count(ts) ct,sum(vc) from sensor group by id ");

        tableEnv.toRetractStream(result, Row.class).print();

        env.execute();

    }

    @Test
    public void ddl() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);


        String bootstrap = "kudu1:9092";

        tableEnv.executeSql("create table source_sensor(id string, ts bigint,vc int) with ("
                + "'connector'='kafka'"
                + ",'topic'='topic_source1'"
                + ",'properties.bootstrap.servers'='" + bootstrap + "'"
                + ",'scan.startup.mode'='earliest-offset'"
                + ",'value.format'='json'"
                + " )"
        );

        tableEnv.executeSql("create table topic_sink(id string, ts bigint,vc int) with ("
                + "'connector'='kafka'"
                + ",'topic'='topic_sink'"
                + ",'properties.bootstrap.servers'='" + bootstrap + "'"
                + ",'format'='json'"
                + " )"
        );

        tableEnv.executeSql("create table topic_sink1(id string, ts bigint,vc int) with ("
                + "'connector'='kafka'"
                + ",'topic'='topic_sink'"
                + ",'properties.bootstrap.servers'='" + bootstrap + "'"
                + ",'format'='json'"
                + " )"
        );


        tableEnv.executeSql("insert into topic_sink select * from source_sensor");

        tableEnv.executeSql("insert into topic_sink1 select * from source_sensor");


        while (true) {

        }
    }

}
