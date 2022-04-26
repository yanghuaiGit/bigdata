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

package sql.window.overwindow;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.rowInterval;

public class Unbound {
    @Test
    public void over() throws Exception {
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

        //开启over无界窗口 全局的数据累加
        Table select = table.window(Over.partitionBy("id").orderBy($("pt")).as("ow"))
                .select($("id"), $("vc").sum().over($("ow")));

        //结果转为流进行输出 追加流，一个窗口只计算一次
        tableEnv.toAppendStream(select, Row.class).print();
        env.execute();
    }


    @Test
    public void preceding() throws Exception {
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

        //开启over无界窗口 只取前面2行 加当前一行 其实也就是3行
        Table select = table.window(Over.partitionBy("id").orderBy($("pt")).preceding(rowInterval(2L)).as("ow"))
                .select($("id"), $("vc").sum().over($("ow")));

        //结果转为流进行输出 追加流，一个窗口只计算一次
        tableEnv.toAppendStream(select, Row.class).print();
        env.execute();
    }

    @Test
    public void sql() throws Exception {
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

//        Table select = tableEnv.sqlQuery("select id , sum(vc) over (partition by id order by pt) sum_vc, count(id)  over (partition by id order by pt) count_id from " +  table );
//        这个语法会报错，2个over里的内容需要保持一致
//        Table select = tableEnv.sqlQuery("select id , sum(vc) over (partition by id order by pt) sum_vc, count(id)  over (order by pt) count_id from " +  table );
//        Table select = tableEnv.sqlQuery("select id , sum(vc) over w sum_vc, count(id)  over w count_id from " +  table +" window w as  (partition by id order by pt)" );
      //前2行和当前行
        Table select = tableEnv.sqlQuery("select id , sum(vc) over w sum_vc, count(id)  over w count_id from " +  table +" window w as  (partition by id order by pt rows between 2 preceding and current row)" );
//        //开启over无界窗口 只取前面2行 加当前一行 其实也就是3行
//        Table select = table.window(Over.partitionBy("id").orderBy($("pt")).preceding(rowInterval(2L)).as("ow"))
//                .select($("id"), $("vc").sum().over($("ow")));

        //结果转为流进行输出 追加流，一个窗口只计算一次
//        tableEnv.toAppendStream(select, Row.class).print();
        select.execute().print();
        env.execute();
    }



}
