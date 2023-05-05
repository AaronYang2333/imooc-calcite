import function.UDF;
import function.UDTF;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utils.ResultSetUtil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

/**
 * @author AaronY
 * @version 1.0
 * @since 2023/5/5
 */
public class QueryJDBC {

    private final String DEFAULT_SCHEMA = "postgres";

    private DataSource postgresDataSource;

    private CalciteConnection calciteConnection;

    @BeforeEach
    public void init() throws Exception {
        postgresDataSource = JdbcSchema.dataSource(
                "jdbc:postgresql://10.5.24.18:2345/aiworks?stringtype=unspecified",
                "org.postgresql.Driver", // Change this if you want to use something like MySQL, Oracle, etc.
                "gpadmin", // username
                "gp2020"   // password
        );

        Properties config = new Properties();
        config.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.FALSE.toString());
        Connection connection = DriverManager.getConnection("jdbc:calcite:", config);
        calciteConnection = connection.unwrap(CalciteConnection.class);

        JdbcSchema pgSchema = JdbcSchema.create(calciteConnection.getRootSchema(), DEFAULT_SCHEMA, postgresDataSource, null, "dataset");

        calciteConnection.getRootSchema().add(DEFAULT_SCHEMA, pgSchema);
    }

    @Test
    void query() throws SQLException {
        String sql = String.format("select * from %s._iris_ limit 10", DEFAULT_SCHEMA);
        ResultSet resultSet = calciteConnection.createStatement().executeQuery(sql);
        System.out.println(ResultSetUtil.resultString(resultSet, true));
    }

    @Test
    void join() throws SQLException {
        String mysqlSchemaName = "mysql";
        DataSource mysqlDataSource = JdbcSchema.dataSource(
                "jdbc:mysql://10.5.24.98:3306/test?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true&useAffectedRows=true&serverTimezone=Asia/Shanghai&useSSL=false",
                "com.mysql.cj.jdbc.Driver", // Change this if you want to use something like MySQL, Oracle, etc.
                "root", // username
                "Zhejianglab@123"   // password
        );
        JdbcSchema mysqlSchema = JdbcSchema.create(calciteConnection.getRootSchema(), mysqlSchemaName,
                mysqlDataSource, null, "test");

        calciteConnection.getRootSchema().add(mysqlSchemaName, mysqlSchema);
        String sql = String.format("select * from mysql.test_info a " +
                "right join %s._iris_ b on a.id = b.target limit 10", DEFAULT_SCHEMA);

        ResultSet resultSet = calciteConnection.createStatement().executeQuery(sql);

        Assertions.assertNotNull(resultSet);
        Assertions.assertEquals(9, ResultSetUtil.resultList(resultSet).get(0).size());

        System.out.println(ResultSetUtil.resultString(resultSet, true));
    }

    @Test
    void update() throws SQLException {
        String insertSql = String.format("insert into %s._test_semantic values ('111', '222', '333', '444', '555', '666', '777', '888', '999', '1010', '1111')", DEFAULT_SCHEMA);
        calciteConnection.createStatement().execute(insertSql);

        String querySql = String.format("select * from %s._test_semantic where name = '%s'", DEFAULT_SCHEMA, "111");
        ResultSet resultSet = calciteConnection.createStatement().executeQuery(querySql);
        List<List<Object>> valueList = ResultSetUtil.resultList(resultSet);

        System.out.println(valueList);

        String updateSql = String.format("update %s._test_semantic set phone = 'phone_modify' where name = '%s'", DEFAULT_SCHEMA, "111");
        calciteConnection.createStatement().execute(updateSql);

        ResultSet resultSet2 = calciteConnection.createStatement().executeQuery(querySql);
        List<List<Object>> valueList2 = ResultSetUtil.resultList(resultSet2);

        System.out.println(valueList2);

        Assertions.assertEquals(valueList.size(), valueList2.size());
        Assertions.assertEquals(1, IntStream.range(0, valueList.size()).boxed()
                .filter(index -> !valueList.get(index).equals(valueList2.get(index))).count());

        String deleteSql = String.format("delete from %s._test_semantic where name = '111'", DEFAULT_SCHEMA);
        calciteConnection.createStatement().execute(deleteSql);
    }

    @Test
    void execUDF() throws SQLException {
        calciteConnection.getRootSchema().add("split", ScalarFunctionImpl.create(UDF.class, UDF.EVAL_METHOD));
        String sql = String.format("select name, split(email) from %s._test_semantic limit 10 offset 24", DEFAULT_SCHEMA);
        ResultSet resultSet = calciteConnection.createStatement().executeQuery(sql);
        System.out.println(ResultSetUtil.resultString(resultSet, true));
    }

    @Test
    void execUDF2() throws SQLException {
        calciteConnection.getRootSchema().getSubSchema(DEFAULT_SCHEMA).add("split", ScalarFunctionImpl.create(UDF.class, UDF.EVAL_METHOD));
        String sql = String.format("select name, split(email) from %s._test_semantic limit 10 offset 24", DEFAULT_SCHEMA);

        Assertions.assertThrows(SQLException.class, () -> calciteConnection.createStatement().executeQuery(sql), "cannot found function");

        String modifiedSql = String.format("select name, %s.split(email) from %s._test_semantic limit 10 offset 24", DEFAULT_SCHEMA, DEFAULT_SCHEMA);
        ResultSet resultSet = calciteConnection.createStatement().executeQuery(modifiedSql);
        System.out.println(ResultSetUtil.resultString(resultSet, true));
    }


    @Test
    void execUDF3() throws SQLException {
        calciteConnection.getRootSchema().add("expand", ScalarFunctionImpl.create(UDTF.class, UDTF.EVAL_METHOD));
        String sql = String.format("select name, expand(email) from %s._test_semantic limit 10 offset 24", DEFAULT_SCHEMA);
        ResultSet resultSet = calciteConnection.createStatement().executeQuery(sql);
        System.out.println(ResultSetUtil.resultString(resultSet, true));
    }


}
