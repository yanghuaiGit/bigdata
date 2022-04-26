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

package calcite.entity;

import com.sun.deploy.util.StringUtils;
import org.apache.calcite.config.Lex;
import org.apache.calcite.server.ServerDdlExecutor;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MysqlCalciteSqlDdlConvent extends BaseCalciteDdlConvent {
    private String template = "CREATE TABLE `%s`.`%s` (%s) \n";


    //ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    private Properties properties;
    public MysqlCalciteSqlDdlConvent(Properties properties) {

        super(new MysqlType(),  SqlParser.configBuilder()
                .setLex(Lex.MYSQL)
                .setParserFactory(ServerDdlExecutor.PARSER_FACTORY).build());
        this.properties = properties;
    }


    protected String doParseCreateTableDataToSql(Ddldata ddldata) {
        CreateTableData d = (CreateTableData) ddldata;
        ArrayList<String> columnS = new ArrayList<>();
        for (ColumnData columnData : d.getColumnDataList()) {
            StringBuilder columnBuilder = new StringBuilder();
            columnBuilder.append("`").append(columnData.getName()).append("`")
                    .append(" ")
                    .append(typeConvent.conventExternal(columnData.getType()))
                    .append(" ")
            ;
            if (!columnData.isNullable()) {
                columnBuilder.append(" ").append("NOT NULL").append(" ");
            }

            if (columnData.getDefaultValue() != null) {
                columnBuilder.append("DEFAULT").append(" ").append(columnData.getDefaultValue()).append(" ");
            }

            if (columnData.getComment() != null) {
                columnBuilder.append("COMMENT").append(" ").append("'").append(columnData.getComment()).append("'");
            }
            columnS.add(columnBuilder.toString());
        }

        String createDdl = String.format(template, d.getSchema(), d.getTable(), "\n" + StringUtils.join(columnS, ",\n"));
        if (!properties.isEmpty()) {
            String propertiesString = "";
            for (Map.Entry<Object, Object> objectObjectEntry : properties.entrySet()) {
                String key = objectObjectEntry.getKey().toString();
                String value = objectObjectEntry.getValue().toString();
                propertiesString = propertiesString + " " + key + " " + value;
            }

            createDdl = createDdl + propertiesString;
        }

        return createDdl;
    }


    protected Ddldata doParseCreateTableSqlToData(SqlNode sqlNode, RowData row) {
        // 解析配置

        SqlCreateTable sqlCreateTableNode = (SqlCreateTable) sqlNode;
        sqlCreateTableNode.toSqlString(MysqlSqlDialect.DEFAULT);
        List<SqlNode> columns = sqlCreateTableNode.columnList.getList();
        ArrayList<ColumnData> columnData1 = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            SqlColumnDeclaration columnDeclaration = (SqlColumnDeclaration) columns.get(i);
            String name = columnDeclaration.name.toString();
            SqlDataTypeSpec dataType = columnDeclaration.dataType;
            Boolean nullable = dataType.getNullable();
            SqlBasicTypeNameSpec typeNameSpec = (SqlBasicTypeNameSpec) dataType.getTypeNameSpec();
            SqlIdentifier typeName = typeNameSpec.getTypeName();
            String defaultValiue = null;
            switch (columnDeclaration.strategy.toString()) {
                case "DEFAULT":
                    if (!((SqlLiteral) columnDeclaration.expression).getTypeName().name().equals("NULL")) {
                        defaultValiue = columnDeclaration.expression.toString();
                    }
                    break;
            }
            MysqlType mysqlType = new MysqlType();
            Type convent = mysqlType.conventInternal(typeName.toString());

            ColumnData columnData = new ColumnData(name, convent, nullable, false, defaultValiue, null, 255, -1, -1);
            columnData1.add(columnData);

        }
        DdlRowData ddlRowData = (DdlRowData) row;
        String schema = "";
        String table = "";
        for (int i = 0; i < ddlRowData.getHeaders().length; i++) {
            if (ddlRowData.getHeaders()[i].equals("schema")) {
                schema = ddlRowData.getInfo(i);
            }
            if (ddlRowData.getHeaders()[i].equals("table")) {
                table = ddlRowData.getInfo(i);
            }
        }
        return new CreateTableData(schema, table, columnData1, EventType.CREATE_TABLE, sqlNode.toSqlString(MysqlSqlDialect.DEFAULT).getSql());
    }

    @Override
    public String getDataSource() {
        return  "MYSQL";
    }
}
