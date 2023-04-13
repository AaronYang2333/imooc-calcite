package utils;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import java.net.URISyntaxException;

public class SqlToRelNode {
    public static SqlToRelConverter createSqlToRelConverter(SqlParser.Config parserConfig,
                                                            SqlToRelConverter.Config sqlToRelConverterConfig,
                                                            RelOptPlanner planner) throws URISyntaxException {

        PlannerImpl plannerImpl = new PlannerImpl(Frameworks
                .newConfigBuilder()
                .sqlToRelConverterConfig(sqlToRelConverterConfig)
                .parserConfig(parserConfig)
                .build());
        SqlTypeFactoryImpl factory = new JavaTypeFactoryImpl();

        RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        CalciteCatalogReader catalogReader = CatalogReaderUtil.createCatalogReader(parserConfig);
        final SqlStdOperatorTable instance = SqlStdOperatorTable.instance();
        SqlValidator validator = SqlValidatorUtil.newValidator(SqlOperatorTables.chain(instance, catalogReader),
                catalogReader, factory, SqlValidator.Config.DEFAULT.withIdentifierExpansion(true));

        return new SqlToRelConverter(
                plannerImpl,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                sqlToRelConverterConfig);
    }

    public static RelRoot createRelRoot(SqlParser.Config parserConfig,
                                        SqlToRelConverter.Config sqlToRelConverterConfig,
                                        RelOptPlanner planner,
                                        SqlNode sqlQuery) throws URISyntaxException {
        SqlToRelConverter sqlToRelConverter = createSqlToRelConverter(parserConfig,
                sqlToRelConverterConfig, planner);
        return sqlToRelConverter.convertQuery(sqlQuery, false, true);
    }

    public static RelNode getSqlNode(String sql, RelOptPlanner relOptPlanner) throws SqlParseException, URISyntaxException {
        final FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder().build();
        final Planner planner = Frameworks.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse(sql);
        SqlParser.Config sqlConfig = SqlParser.config().withLex(Lex.MYSQL).withCaseSensitive(false);
        SqlToRelConverter.Config config = SqlToRelConverter.config();
        return SqlToRelNode.createRelRoot(sqlConfig, config, relOptPlanner, sqlNode).rel;
    }
}
