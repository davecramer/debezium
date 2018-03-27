/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.Types;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import io.debezium.connector.oracle.parser.PlSqlLexer;
import io.debezium.connector.oracle.parser.PlSqlParser;
import io.debezium.connector.oracle.parser.PlSqlParser.Column_definitionContext;
import io.debezium.connector.oracle.parser.PlSqlParser.Column_nameContext;
import io.debezium.connector.oracle.parser.PlSqlParser.Create_tableContext;
import io.debezium.connector.oracle.parser.PlSqlParser.Out_of_line_constraintContext;
import io.debezium.connector.oracle.parser.PlSqlParser.Precision_partContext;
import io.debezium.connector.oracle.parser.PlSqlParser.Tableview_nameContext;
import io.debezium.connector.oracle.parser.PlSqlParser.Unit_statementContext;
import io.debezium.connector.oracle.parser.PlSqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParser;
import oracle.jdbc.OracleTypes;

public class OracleDdlParser implements DdlParser {

    private String catalogName;
    private String schemaName;

    @Override
    public void setCurrentDatabase(String databaseName) {
        this.catalogName = databaseName;
    }

    @Override
    public void setCurrentSchema(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public DdlChanges getDdlChanges() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public String terminator() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public SystemVariables systemVariables() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void parse(String ddlContent, Tables databaseTables) {
        if (!ddlContent.endsWith(";")) {
            ddlContent = ddlContent + ";";
        }

        try {
            PlSqlLexer lexer = new PlSqlLexer(new ANTLRInputStream(toUpperCase(ddlContent)));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            PlSqlParser parser = new PlSqlParser(tokens);

            Unit_statementContext ast = parser.unit_statement();
            CreateTableListener createTablelistener = new CreateTableListener();
            ParseTreeWalker.DEFAULT.walk(createTablelistener, ast);

            if (createTablelistener.getTable() != null) {
                databaseTables.overwriteTable(createTablelistener.getTable());
            }
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Couldn't parse DDL statement " + ddlContent, e);
        }
    }

    // TODO excluded quoted identifiers
    private String toUpperCase(String ddl) {
        return ddl.toUpperCase(Locale.ENGLISH);
    }

    private class CreateTableListener extends PlSqlParserBaseListener {

        private TableEditor editor;

        public Table getTable() {
            return editor != null ? editor.create() : null;
        }

        @Override
        public void enterCreate_table(Create_tableContext ctx) {
            if (ctx.relational_table() == null) {
                throw new IllegalArgumentException("Only relational tables are supported");
            }

            editor = Table.editor();
            editor.tableId(new TableId(catalogName, schemaName, getTableName(ctx.tableview_name())));

            super.enterCreate_table(ctx);
        }

        private String getTableName(Tableview_nameContext tableview_name) {
            if (tableview_name.id_expression() != null) {
                return tableview_name.id_expression().getText();
            }
            else {
                return tableview_name.identifier().id_expression().getText();
            }
        }

        @Override
        public void exitColumn_definition(Column_definitionContext ctx) {
            ColumnEditor columnEditor = Column.editor();
            columnEditor.name(getColumnName(ctx.column_name()));

            if (ctx.datatype().native_datatype_element().INT() != null
                    || ctx.datatype().native_datatype_element().INTEGER() != null
                    || ctx.datatype().native_datatype_element().SMALLINT() != null) {
                columnEditor.jdbcType(Types.NUMERIC);
                columnEditor.type("NUMBER");
                columnEditor.length(38);
                columnEditor.scale(0);
            }
            else if (ctx.datatype().native_datatype_element().DATE() != null) {
                // JDBC driver reports type as timestamp but name DATE
                columnEditor.jdbcType(Types.TIMESTAMP);
                columnEditor.type("DATE");
            }
            else if (ctx.datatype().native_datatype_element().TIMESTAMP() != null) {
                if (ctx.datatype().WITH() != null
                        && ctx.datatype().TIME() != null
                        && ctx.datatype().ZONE() != null) {
                    if (ctx.datatype().LOCAL() != null) {
                        columnEditor.jdbcType(OracleTypes.TIMESTAMPLTZ);
                        columnEditor.type("TIMESTAMP WITH LOCAL TIME ZONE");
                    }
                    else {
                        columnEditor.jdbcType(OracleTypes.TIMESTAMPTZ);
                        columnEditor.type("TIMESTAMP WITH TIME ZONE");
                    }
                }
                else {
                    columnEditor.jdbcType(Types.TIMESTAMP);
                    columnEditor.type("TIMESTAMP");
                }
                columnEditor.length(6);
            }
            else if (ctx.datatype().native_datatype_element().VARCHAR2() != null) {
                columnEditor.jdbcType(Types.VARCHAR);
                columnEditor.type("VARCHAR2");
                columnEditor.length(getVarCharDefaultLength());
            }
            else if (ctx.datatype().native_datatype_element().NVARCHAR2() != null) {
                columnEditor.jdbcType(Types.NVARCHAR);
                columnEditor.type("NVARCHAR2");
                columnEditor.length(getVarCharDefaultLength());
            }
            else if (ctx.datatype().native_datatype_element().CHAR() != null) {
                columnEditor.jdbcType(Types.CHAR);
                columnEditor.type("CHAR");
                columnEditor.length(1);
            }
            else if (ctx.datatype().native_datatype_element().NCHAR() != null) {
                columnEditor.jdbcType(Types.NCHAR);
                columnEditor.type("NCHAR");
                columnEditor.length(1);
            }
            else if (ctx.datatype().native_datatype_element().BINARY_FLOAT() != null) {
                columnEditor.jdbcType(OracleTypes.BINARY_FLOAT);
                columnEditor.type("BINARY_FLOAT");
            }
            else if (ctx.datatype().native_datatype_element().BINARY_DOUBLE() != null) {
                columnEditor.jdbcType(OracleTypes.BINARY_DOUBLE);
                columnEditor.type("BINARY_DOUBLE");
            }
            else if (ctx.datatype().native_datatype_element().FLOAT() != null) {
                columnEditor.jdbcType(Types.FLOAT);
                columnEditor.type("FLOAT");
                columnEditor.length(126);
            }
            else if (ctx.datatype().native_datatype_element().NUMERIC() != null
                    || ctx.datatype().native_datatype_element().NUMBER() != null
                    || ctx.datatype().native_datatype_element().DECIMAL() != null) {
                columnEditor.jdbcType(Types.NUMERIC);
                columnEditor.type("NUMBER");
            }
            else {
                throw new IllegalArgumentException("Unsupported column type: " + ctx.datatype().native_datatype_element().getText());
            }

            Precision_partContext precisionPart = ctx.datatype().precision_part();
            if (precisionPart != null) {
                columnEditor.length(Integer.valueOf(precisionPart.numeric(0).getText()));

                if (precisionPart.numeric().size() > 1) {
                    columnEditor.scale(Integer.valueOf(precisionPart.numeric(1).getText()));
                }
            }

            boolean hasNotNullConstraint = ctx.inline_constraint().stream()
                .filter(c -> c.NOT() != null)
                .findFirst()
                .isPresent();

            columnEditor.optional(!hasNotNullConstraint);

            editor.addColumn(columnEditor.create());

            super.exitColumn_definition(ctx);
        }

        private int getVarCharDefaultLength() {
            // TODO replace with falue from select name, value  from v$parameter where name='max_string_size';
            return 4000;
        }

        @Override
        public void exitOut_of_line_constraint(Out_of_line_constraintContext ctx) {
            if(ctx.PRIMARY() != null) {
                List<String> pkColumnNames = ctx.column_name().stream()
                    .map(this::getColumnName)
                    .collect(Collectors.toList());

                editor.setPrimaryKeyNames(pkColumnNames);
            }

            super.exitOut_of_line_constraint(ctx);
        }

        private String getColumnName(Column_nameContext ctx) {
            return ctx.identifier().id_expression().getText();
        }
    }
}
