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

package catalog;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.junit.Test;

/**
 * @author dujie
 * @Description
 * @createTime 2022-01-22 17:06:00
 */
public class HiveCataLogTest {

    @Test
    public void test1(){
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        /**
         * 创建 hivecatalog
         *
         * my_hive catalog 名称（随意
         * gmall 指定数据库
         * input/ 配置hive-site.xml 的目录。
         */
        HiveCatalog hiveCatalog = new HiveCatalog("my_hive", "default", "src/main/resources/conf");

        /**
         * 注册Catalog
         * my_hive 名称随意，通常和  new HiveCatalog 中的一致
         */
        tableEnv.registerCatalog("my_hive",hiveCatalog);

        tableEnv.useCatalog("my_hive");
        // 选择 database
//        tableEnv.useDatabase("default");

        /**
         * 指定数据库,查询
         * my_hive tableEnv.registerCatalog 中指定的
         * gmall ：库名
         * student：表名
         */
        tableEnv.sqlQuery("select * from orc_test1").execute().print();

    }
}
