import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

public class Lineage {


    private static RelRoot relRoot;

    public static class TestSchema {
        public final Triple[] rdf = {new Triple("s", "p", "o")};
    }

    public static void main(String[] args) {
        SchemaPlus schemaPlus = Frameworks.createRootSchema(true);

        schemaPlus.add("T", new ReflectiveSchema(new TestSchema()));
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        configBuilder.defaultSchema(schemaPlus);

        FrameworkConfig frameworkConfig = configBuilder.build();

        Planner planner = Frameworks.getPlanner(frameworkConfig);

        SqlNode sqlNode;
        RelRoot relRoot = null;
        try {
            sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"");
            planner.validate(sqlNode);
            relRoot = planner.rel(sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
        }

        RelNode relNode = relRoot.project();
        System.out.print(RelOptUtil.toString(relNode));
    }

    public static class Triple {
        public String s;
        public String p;
        public String o;

        public Triple(String s, String p, String o) {
            super();
            this.s = s;
            this.p = p;
            this.o = o;
        }
    }

}
