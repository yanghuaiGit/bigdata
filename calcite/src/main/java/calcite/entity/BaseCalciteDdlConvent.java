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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.table.data.RowData;

public abstract class BaseCalciteDdlConvent implements DdlConvent {

    protected final TypeConvent typeConvent;
    // 解析配置
    protected final SqlParser.Config sqlParseConfig;

    public BaseCalciteDdlConvent(TypeConvent typeConvent, SqlParser.Config sqlParseConfig) {
        this.typeConvent = typeConvent;
        this.sqlParseConfig = sqlParseConfig;
    }

    @Override
    public String ddlDataConventToSql(Ddldata ddldata) {
        switch (ddldata.getType()) {
            case CREATE_SCHEMA:
                return doParseCreateSchemaDataToSql(ddldata);
            case ALTER_TABLE:
                return doParseAlterTableDataToSql(ddldata);
            case CREATE_TABLE:
                return doParseCreateTableDataToSql(ddldata);
            default:
                throw new UnsupportedOperationException("not support " + ddldata.getType());
        }
    }


    @Override
    public Ddldata rowConventToDdlData(RowData row) {
        String sql = getSql(row);
        // 创建解析器
        SqlParser parser = SqlParser.create(sql, sqlParseConfig);
        SqlNode sqlNode;
        try {
            // 解析sql
            sqlNode = parser.parseStmt();
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }

        switch (sqlNode.getKind().toString()) {
            case "CREATE_TABLE":
                return doParseCreateTableSqlToData(sqlNode, row);
            case "CREATE_SCHEMA":
                return doParseCreateSchemaSqlToData(sqlNode, row);
            case "ALTER_TABLE":
                return doParseAlterTableSqlToData(sqlNode, row);
            default:
                throw new RuntimeException("not support parse sql type " + sqlNode.getKind());
        }
    }

    protected Ddldata doParseCreateTableSqlToData(SqlNode sql, RowData row) {
        throw new UnsupportedOperationException("not support CreateTable ");
    }

    protected Ddldata doParseAlterTableSqlToData(SqlNode sql, RowData row) {
        throw new UnsupportedOperationException("not support AlterTable ");
    }

    protected Ddldata doParseCreateSchemaSqlToData(SqlNode sql, RowData row) {
        throw new UnsupportedOperationException("not support  CreateSchema");
    }

    protected String doParseAlterTableDataToSql(Ddldata ddldata) {
        throw new UnsupportedOperationException("not support " + ddldata.getType());
    }

    protected String doParseCreateSchemaDataToSql(Ddldata ddldata) {
        throw new UnsupportedOperationException("not support " + ddldata.getType());
    }

    protected String doParseCreateTableDataToSql(Ddldata ddldata) {
        throw new UnsupportedOperationException("not support " + ddldata.getType());
    }


    private String getSql(RowData row) {
        DdlRowData ddlRowData = (DdlRowData) row;
        String sql = "";
        for (int i = 0; i < ddlRowData.getHeaders().length; i++) {
            if (ddlRowData.getHeaders()[i].equals("sql")) {
                sql = ddlRowData.getInfo(i);
            }
        }
        return sql;
    }

}
