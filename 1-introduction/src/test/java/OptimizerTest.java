import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

public class OptimizerTest {
    @Test
    public void test_optimizer() throws Exception {
        SimpleTable users = SimpleTable.newBuilder("users")
                .addField("id", SqlTypeName.BIGINT)
                .addField("name", SqlTypeName.VARCHAR)
                .addField("age", SqlTypeName.INTEGER)
                .addField("birthday", SqlTypeName.DATE)
                .withRowCount(30L)
                .build();

        SimpleTable orders = SimpleTable.newBuilder("orders")
                .addField("id", SqlTypeName.BIGINT)
                .addField("user_id", SqlTypeName.BIGINT)
                .addField("amount", SqlTypeName.DECIMAL)
                .addField("total", SqlTypeName.DECIMAL)
                .addField("created", SqlTypeName.DATE)
                .withRowCount(90L)
                .build();

        SimpleSchema schema = SimpleSchema.newBuilder("test")
                .addTable(users)
                .addTable(orders)
                .build();

        Optimizer optimizer = Optimizer.create(schema);

        String sql = "SELECT u.id AS user_id, u.name AS user_name, o.id as order_id" +
                "    FROM test.users u JOIN test.orders o ON u.id = o.user_id" +
                "    WHERE u.age > 50";

        SqlNode sqlTree = optimizer.parse(sql);
        SqlNode validatedSqlTree = optimizer.validate(sqlTree);
        RelNode relTree = optimizer.convert(validatedSqlTree);

        print("BEFORE CONVERSION", relTree);

        RuleSet rules = RuleSets.ofList(
                CoreRules.FILTER_TO_CALC,
                CoreRules.PROJECT_TO_CALC,
                CoreRules.FILTER_CALC_MERGE,
                CoreRules.PROJECT_CALC_MERGE,
                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                EnumerableRules.ENUMERABLE_PROJECT_RULE,
                EnumerableRules.ENUMERABLE_FILTER_RULE,
                EnumerableRules.ENUMERABLE_CALC_RULE,
                EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                EnumerableRules.ENUMERABLE_JOIN_RULE
        );

        RelNode optimizerRelTree = optimizer.optimize(
                relTree,
                relTree.getTraitSet().plus(EnumerableConvention.INSTANCE),
                rules
        );

        print("AFTER OPTIMIZATION", optimizerRelTree);
    }

    private void print(String header, RelNode relTree) {
        StringWriter sw = new StringWriter();

        sw.append(header).append(":").append("\n");

        RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, true);

        relTree.explain(relWriter);

        System.out.println(sw.toString());

        RelToSqlConverter relToSqlConverter = new RelToSqlConverter(CalciteSqlDialect.DEFAULT);
        SqlImplementor.Result visit = relToSqlConverter.visitRoot(relTree);
        SqlNode sqlNode = visit.asStatement();
        System.out.println(sqlNode.toSqlString(CalciteSqlDialect.DEFAULT).getSql());
        System.out.println();
    }
}
