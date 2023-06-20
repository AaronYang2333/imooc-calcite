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
public class LocalServer {

    public static void main(String[] args) throws Exception {
        int port = 8765;

//        final HttpServer server = org.apache.calcite.avatica.server.Main.start(
//                new String[]{JdbcMeta.class.getName()}, port, AvaticaJsonHandler::new);
//
//        server.start();
//        server.join();
    }

    private static String readAuthProperties() throws UnsupportedEncodingException {
        return URLDecoder.decode(RemoteServer.class.getResource("/auth-users.properties").getFile(), "UTF-8");
    }
}
