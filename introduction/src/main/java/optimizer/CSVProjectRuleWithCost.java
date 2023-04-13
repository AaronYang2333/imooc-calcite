package optimizer;

import csvRelNode.CSVProjectWithCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;

public class CSVProjectRuleWithCost extends RelRule<CSVProjectRuleWithCost.Config> {

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalProject project = call.rel(0);
        final RelNode converted = convert(project);
        if (converted != null) {
            call.transformTo(converted);
        }
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
//        Config DEFAULT = EMPTY
//                .withOperandSupplier(b0 ->
//                        b0.operand(LogicalProject.class).anyInputs())
//                .as(Config.class);

        @Override default CSVProjectRuleWithCost toRule() {
            return new CSVProjectRuleWithCost(this);
        }
    }

    private CSVProjectRuleWithCost(Config config) {
        super(config);
    }


    public RelNode convert(RelNode rel) {
        final LogicalProject project = (LogicalProject) rel;
        final RelTraitSet traitSet = project.getTraitSet();
        return new CSVProjectWithCost(project.getCluster(), traitSet,
                project.getInput(), project.getProjects(),
                project.getRowType());
    }
}
