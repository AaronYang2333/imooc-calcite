import org.apache.calcite.adapter.csv.CsvSchema;
import org.apache.calcite.adapter.csv.CsvTable;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import utils.ResultSetUtil;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

/**
 * @author AaronY
 * @version 1.0
 * @since 2022/11/10
 */
public class QueryCSV {
    private CalciteConnection calciteConnection;

    @BeforeEach
    void init() throws SQLException {
        String fileDirPath = Objects.requireNonNull(QueryCSV.class.getClassLoader().getResource("csv").getPath());
        Properties info = new Properties();
        info.setProperty("caseSensitive", "false");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        calciteConnection = connection.unwrap(CalciteConnection.class);
        CsvSchema csvSchema = new CsvSchema(new File(fileDirPath), CsvTable.Flavor.SCANNABLE);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("csv", csvSchema);
    }

    @Test
    void groupByQuery() throws SQLException {
        String sql = "select EMPNO, max(AGE) from csv.depts group by EMPNO";
        ResultSet resultSet = calciteConnection.createStatement().executeQuery(sql);
        System.out.println(ResultSetUtil.resultString(resultSet));
    }


    @Test
    void join() throws SQLException {
        String mysqlSchemaName = "mysql";
        DataSource mysqlDataSource = JdbcSchema.dataSource(
                "jdbc:mysql://10.105.20.64:32307/test?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true&useAffectedRows=true&serverTimezone=Asia/Shanghai&useSSL=false",
                "com.mysql.cj.jdbc.Driver", // Change this if you want to use something like MySQL, Oracle, etc.
                "root", // username
                "rPlHyxKEVp"   // password
        );
        JdbcSchema mysqlSchema = JdbcSchema.create(calciteConnection.getRootSchema(), mysqlSchemaName,
                mysqlDataSource, null, "test");

        calciteConnection.getRootSchema().add(mysqlSchemaName, mysqlSchema);
        String sql = "select * from mysql.employees a right join csv.depts b on a.dept_no = b.deptno limit 10";

        ResultSet resultSet = calciteConnection.createStatement().executeQuery(sql);

        System.out.println(ResultSetUtil.resultString(resultSet, true));
    }




    @Test
    void query() throws SQLException, IOException {
        String sql = "select empno,NAME from csv.depts where EMPNO = 110";
        ResultSet resultSet = calciteConnection.createStatement().executeQuery(sql);
        Assertions.assertEquals(2, ResultSetUtil.resultList(resultSet).size());
        File tempFile = File.createTempFile("output", ".csv", new File(QueryCSV.class.getClassLoader().getResource("csv").getPath()));
        Arrays.stream(new File(QueryCSV.class.getClassLoader().getResource("csv").getPath()).listFiles()).
                forEach(file -> System.out.println(file.getName()));
        String sql2 = String.format("select empno,NAME from csv.%s where EMPNO = 110", tempFile.getName().split("\\.")[0]);
        System.out.println(sql2);
        Executable task = () -> calciteConnection.createStatement().executeQuery(sql2);
        Assertions.assertThrows(SQLException.class, task, "cannot find table");
    }

    /**
     * failed
     *
     * @throws SQLException
     */
    @Test
    void update() throws Throwable {
        String updateSql = "update csv.depts set DEPTNO = 66 where EMPNO = 130";
        Executable task = () -> calciteConnection.createStatement().execute(updateSql);
        Assertions.assertThrows(SQLException.class, task, "There are not enough rules to produce a node with desired properties");

        String sql = "select * from csv.depts where EMPNO = 130";
        ResultSet resultSet = calciteConnection.createStatement().executeQuery(sql);
    }


}
