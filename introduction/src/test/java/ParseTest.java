import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

/**
 * @author AaronY
 * @version 1.0
 * @since 2023/4/7
 */
public class ParseTest {

    @Test
    void parsePlainSQL() throws SqlParseException {
        // Sql语句
        String sql = "select name, age from t_user where gender = M";
        // 解析配置
        SqlParser.Config mysqlConfig = SqlParser.config()
                .withParserFactory(SqlParser.Config.DEFAULT.parserFactory())
                .withLex(Lex.MYSQL);
        // 创建解析器
        SqlParser parser = SqlParser.create(sql, mysqlConfig);
        // 解析sql
        SqlNode sqlNode = parser.parseQuery();
        System.out.println("sql in MYSQL dialect -> " + sqlNode.toString());
        System.out.println();
        // 还原某个方言的SQL
        System.out.println("sql in ORACLE dialect -> " + sqlNode.toSqlString(OracleSqlDialect.DEFAULT));
    }

    //TODO
//    @Test
    void parseLOAD() throws SqlParseException {
        // Sql语句
        String sql = "LOAD hdfs:'/data/user.txt' TO mysql:'db.t_user' (c1 c2,c3 c4) SEPARATOR ','";
        // 解析配置
        SqlParser.Config mysqlConfig = SqlParser.config()
                .withParserFactory(SqlParser.Config.DEFAULT.parserFactory())
                .withLex(Lex.MYSQL);
        // 创建解析器
        SqlParser parser = SqlParser.create(sql, mysqlConfig);
        // 解析sql
        SqlNode sqlNode = parser.parseQuery();
        System.out.println(sqlNode.toString());
        // 还原某个方言的SQL
        System.out.println(sqlNode.toSqlString(OracleSqlDialect.DEFAULT));
    }
}
