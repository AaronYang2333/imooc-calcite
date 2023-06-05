import com.google.common.base.Joiner;
import org.apache.calcite.adapter.csv.CsvSchema;
import org.apache.calcite.adapter.csv.CsvTable;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.Test;
import utils.ResultSetUtil;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.IntStream;

public class LineageTest {

    @Test
    void join() throws SQLException {
        String fileDirPath = Objects.requireNonNull(LineageTest.class.getClassLoader().getResource("csv").getPath());
        Properties info = new Properties();
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        CsvSchema csvSchema = new CsvSchema(new File(fileDirPath), CsvTable.Flavor.SCANNABLE);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("csv", csvSchema);

        String mysqlSchemaName = "mysql";
        DataSource mysqlDataSource = JdbcSchema.dataSource(
                "jdbc:mysql://10.105.20.64:32307/test?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true&useAffectedRows=true&serverTimezone=Asia/Shanghai&useSSL=false",
                "com.mysql.cj.jdbc.Driver",
                "root", // username
                "rPlHyxKEVp"   // password
        );
        JdbcSchema mysqlSchema = JdbcSchema.create(calciteConnection.getRootSchema(), mysqlSchemaName,
                mysqlDataSource, null, "test");

        calciteConnection.getRootSchema().add(mysqlSchemaName, mysqlSchema);
//        String sql = "select a.\"name\", b.\"MANAGER\" from \"mysql\".\"employees\" a right join \"csv\".\"depts\" b on a.\"dept_no\" = b.\"DEPTNO\" limit 10";
        String sql = "select * from \"mysql\".\"employees\" a right join \"csv\".\"depts\" b on a.\"dept_no\" = b.\"DEPTNO\" limit 10";

        ResultSet resultSet = calciteConnection.createStatement().executeQuery(sql);
        System.out.println(ResultSetUtil.resultString(resultSet, true));


        lineage(calciteConnection.getRootSchema(), sql);

    }

    void lineage(SchemaPlus schemaPlus, String sql) {

        Planner planner = Frameworks.getPlanner(Frameworks.newConfigBuilder().defaultSchema(schemaPlus).build());

        try {
            SqlNode sqlNode = planner.parse(sql);
            planner.validate(sqlNode);
            RelRoot relRoot = planner.rel(sqlNode);

            List<RelDataTypeField> fieldList = relRoot.project().getRowType().getFieldList();
            RelMetadataQuery metadataQuery = relRoot.project().getCluster().getMetadataQuery();

            IntStream.range(0, fieldList.size()).boxed().forEach(index -> {
                RelDataTypeField relDataTypeField = fieldList.get(index);
                RelColumnOrigin columnOrigin = metadataQuery.getColumnOrigin(relRoot.rel, index);
                assert columnOrigin != null;
                System.out.println(relDataTypeField.getName() + " -> " + wrapMsg(columnOrigin));
            });


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String wrapMsg(RelColumnOrigin columnOrigin) {
        Integer ordinal = columnOrigin.getOriginColumnOrdinal();
        RelOptTable originTable = columnOrigin.getOriginTable();
        RelDataTypeField relDataTypeField = originTable.getRowType().getFieldList().get(ordinal);

        return Joiner.on(".").join(originTable.getQualifiedName(), relDataTypeField.getName());
    }
}
