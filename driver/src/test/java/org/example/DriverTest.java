package org.example;

import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Properties;


/**
 * @author AaronY
 * @version 1.0
 * @since 2023/6/20
 */
public class DriverTest {

    @Test
    void connectTest() throws SQLException, ClassNotFoundException {
        Class.forName("org.example.Driver");
        final Properties p = new Properties();
        p.put("avatica_user", "USER1");
        p.put("avatica_password", "password1");
        p.put("serialization", "protobuf");
        try (Connection conn = DriverManager.getConnection("jdbc:ayyy:customize:url=http://10.11.33.132:8765;authentication=DIGEST", p)) {
            final Statement statement = conn.createStatement();
            // 查询数据
            final ResultSet rs1 = statement.executeQuery("SELECT * FROM \"public\".\"eping_data\" LIMIT 1");
            rs1.next();

            ResultSetMetaData metaData = rs1.getMetaData();
            System.out.println(metaData.getColumnCount());
        }
    }
}
