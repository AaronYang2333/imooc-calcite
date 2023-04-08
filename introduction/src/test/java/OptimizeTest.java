import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;
import utils.SqlToRelNode;

import java.net.URISyntaxException;

/**
 * @author AaronY
 * @version 1.0
 * @since 2023/4/7
 */
public class OptimizeTest {

    @Test
    public void testHepPlanner() throws SqlParseException, URISyntaxException {
        final String sql = "select a.Id from csv.dataA as a join csv.dataB as b on a.Id = b.Id where a.Id>1";
        HepProgramBuilder programBuilder = HepProgram.builder();
        HepPlanner hepPlanner =
                new HepPlanner(
                        programBuilder.addRuleInstance(FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT.toRule())
                                .build());
        RelNode relNode = SqlToRelNode.getSqlNode(sql, hepPlanner);
        //未优化算子树结构
        System.out.println(RelOptUtil.toString(relNode));
        //优化后接结果
        RelOptPlanner planner = relNode.getCluster().getPlanner();
        planner.setRoot(relNode);
        RelNode bestExp = planner.findBestExp();
        System.out.println(RelOptUtil.toString(bestExp));

    }
}
