package server.db;

/**
 * 数据库连接配置
 */
public class DatabaseConfig {
    public static final String HOST = "localhost";
    public static final int PORT = 3306;
    public static final String DATABASE = "FishGrabbing";
    public static final String USERNAME = "root";
    public static final String PASSWORD = "caohuamoyu";

    public static String getUrl() {
        return String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf8&allowPublicKeyRetrieval=true",
                HOST, PORT, DATABASE);
    }
}
