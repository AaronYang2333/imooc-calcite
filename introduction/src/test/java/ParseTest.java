import com.google.common.collect.Lists;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author AaronY
 * @version 1.0
 * @since 2023/4/7
 */
@SuppressWarnings("all")
public class ParseTest {

    /**
     * change dialect
     * @throws SqlParseException
     */
    @Test
    void parsePlainSQL() throws SqlParseException {
        String sql = "select id, name, cast(created_at as date) from redshift.users";

        SqlParser.Config mysqlConfig = SqlParser.config()
                .withParserFactory(SqlParser.Config.DEFAULT.parserFactory())
                .withLex(Lex.MYSQL);
        SqlParser parser = SqlParser.create(sql, mysqlConfig);
        SqlNode sqlNode = parser.parseQuery();

        Assertions.assertEquals(sqlNode.toString(),
                "SELECT `id`, `name`, CAST(`created_at` AS DATE)\r\nFROM `redshift`.`users`");

        Assertions.assertEquals(sqlNode.toSqlString(OracleSqlDialect.DEFAULT).toString(),
                "SELECT \"id\", \"name\", CAST(\"created_at\" AS DATE)\r\nFROM \"redshift\".\"users\"");
    }

    @Test
    void parseComponent() throws SqlParseException {
        String sql = "select u.id as user_id, u.name as user_name, o.id as order_id " +
                "from users u inner join orders o on u.id = o.user_id " +
                "where u.id > 50";
        SqlParser.Config mysqlConfig = SqlParser.config()
                .withParserFactory(SqlParser.Config.DEFAULT.parserFactory())
                .withLex(Lex.MYSQL);
        SqlParser parser = SqlParser.create(sql, mysqlConfig);
        SqlNode sqlNode = parser.parseQuery();

        Assertions.assertTrue(sqlNode instanceof SqlSelect);
        SqlSelect sqlSelect = ((SqlSelect) sqlNode);

        Assertions.assertEquals(
                sqlSelect.getSelectList().stream().map(o -> ((SqlBasicCall) o).getOperandList().get(0).toString()).collect(Collectors.toList()),
                Lists.newArrayList("u.id", "u.name", "o.id")
        );

        Assertions.assertEquals(
                sqlSelect.getSelectList().stream().map(o -> ((SqlBasicCall) o).getOperandList().get(1).toString()).collect(Collectors.toList()),
                Lists.newArrayList("user_id", "user_name", "order_id")
        );

        Assertions.assertEquals(sqlSelect.getWhere().toString(),"`u`.`id` > 50");

        Assertions.assertEquals(sqlSelect.getFrom().toString(),
                "SELECT *\r\n" +
                        "FROM `users` AS `u`\r\n" +
                        "INNER JOIN `orders` AS `o` ON `u`.`id` = `o`.`user_id`");
    }

    //TODO
//    @Test
    void parseLOAD() throws SqlParseException {
        String sql = "LOAD hdfs:'/data/user.txt' TO mysql:'db.t_user' (c1 c2,c3 c4) SEPARATOR ','";
        SqlParser.Config mysqlConfig = SqlParser.config()
                .withParserFactory(SqlParser.Config.DEFAULT.parserFactory())
                .withLex(Lex.MYSQL);
        SqlParser parser = SqlParser.create(sql, mysqlConfig);
        SqlNode sqlNode = parser.parseQuery();
        System.out.println(sqlNode.toString());
        System.out.println(sqlNode.toSqlString(OracleSqlDialect.DEFAULT));
    }
}
