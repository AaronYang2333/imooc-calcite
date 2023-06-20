package org.example;

import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.HttpServer;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * @author AaronY
 * @version 1.0
 * @since 2023/6/20
 */
public class RemoteServer {

    public static void main(String[] args) throws Exception {
        int port = 8765;

        String url = "jdbc:postgresql://10.105.20.64:32433/crawler";
        final JdbcMeta meta = new JdbcMeta(url, "postgres", "N3dSMzhtS");
        final LocalService service = new LocalService(meta);

        final HttpServer server = new HttpServer.Builder<>()
                .withPort(port)
                .withHandler(service, Driver.Serialization.PROTOBUF)
                .withDigestAuthentication(readAuthProperties(), new String[]{"users"})
                .build();

        server.start();
        server.join();
    }

    private static String readAuthProperties() throws UnsupportedEncodingException {
        return URLDecoder.decode(RemoteServer.class.getResource("/auth-users.properties").getFile(), "UTF-8");
    }
}
