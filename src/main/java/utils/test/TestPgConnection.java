package utils.test;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

public class TestPgConnection {

    public static void main(String[] args) {
        
        // ---!!! --- 请在这里修改为您自己的数据库信息 --- !!!---
        String dbHost = "49.52.27.34";
        int dbPort = 26000;
        String dbName = "opengauss";
        String dbUser = "dbhammer";
        String dbPassword = "Dbhammer@2026"; // ！！！！替换为您的真实密码！！！！
        // ---!!! --- 修改结束 --- !!!---

        // 1. 构建JDBC URL (注意: 前缀是 jdbc:postgresql://)
        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", dbHost, dbPort, dbName);

        // 2. 定义驱动类名 (注意: 这是PostgreSQL的驱动类)
        String driver = "org.postgresql.Driver";
        
        Connection conn = null;

        System.out.println("Attempting to connect to openGauss using PostgreSQL JDBC Driver...");
        System.out.println("URL: " + jdbcUrl);
        System.out.println("User: " + dbUser);

        try {
            // 3. 加载驱动
            Class.forName(driver);

            // 4. 创建连接
            conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword);
            
            System.out.println("=========================================");
            System.out.println("  Connection succeed! Congratulations! ");
            System.out.println("=========================================");

        } catch (ClassNotFoundException e) {
            System.err.println("PostgreSQL JDBC Driver not found. Check your pom.xml file.");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Connection failed! Please check the details below.");
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                    System.out.println("Connection closed.");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
