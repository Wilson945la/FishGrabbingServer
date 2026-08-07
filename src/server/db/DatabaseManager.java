package server.db;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.sql.*;

public class DatabaseManager {

    public DatabaseManager() {}

    private Connection getConnection() {
        try {
            return DriverManager.getConnection(
                    DatabaseConfig.getUrl(),
                    DatabaseConfig.USERNAME,
                    DatabaseConfig.PASSWORD
            );
        } catch (SQLException e) {
            System.err.println("数据库连接失败: " + e.getMessage());
            return null;
        }
    }

    public boolean testConnection() {
        try (Connection conn = getConnection()) {
            return conn != null && !conn.isClosed() && conn.isValid(5);
        } catch (SQLException e) {
            return false;
        }
    }

    public String register(String username, String account, String password) {
        if (username == null || username.trim().isEmpty()) return "ERROR|用户名不能为空";
        if (account == null || account.trim().isEmpty()) return "ERROR|账号不能为空";
        if (password == null || password.trim().isEmpty()) return "ERROR|密码不能为空";

        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            if (checkNameExists(conn, username)) return "ERROR|用户名已存在，请更换用户名";
            if (checkAccountExists(conn, account)) return "ERROR|该账号已被使用，请更换账号";

            String hashedPassword = sha256(password);
            String sql = "INSERT INTO users (User_name, User_account, User_password, Fish) VALUES (?, ?, ?, 0)";
            int newUserId = -1;
            try (PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                pstmt.setString(1, username);
                pstmt.setString(2, account);
                pstmt.setString(3, hashedPassword);
                pstmt.executeUpdate();
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
                    if (rs.next()) newUserId = rs.getInt(1);
                }
            }
            if (newUserId == -1) return "ERROR|注册失败";

            // 注册成功后自动添加 moyu官方 为好友（直接加，不发申请）
            int officialId = getUserIdByNameOrAccount(conn, "moyu官方");
            if (officialId != -1) {
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT IGNORE INTO friends (User_id, Friend_id) VALUES (?, ?), (?, ?)")) {
                    pstmt.setInt(1, newUserId);
                    pstmt.setInt(2, officialId);
                    pstmt.setInt(3, officialId);
                    pstmt.setInt(4, newUserId);
                    pstmt.executeUpdate();
                } catch (SQLException e) {
                    System.err.println("自动添加 moyu官方 失败: " + e.getMessage());
                }
            }

            return "SUCCESS|" + newUserId;
        } catch (SQLException e) {
            return "ERROR|注册失败: " + e.getMessage();
        }
    }

    public String login(String accountOrName, String password) {
        if (accountOrName == null || accountOrName.trim().isEmpty()) return "ERROR|请输入账号或用户名";
        if (password == null || password.trim().isEmpty()) return "ERROR|请输入密码";

        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            String hashedPassword = sha256(password);
            String sql = "SELECT User_id, User_name, User_account, Fish FROM users WHERE (User_account = ? OR User_name = ?) AND User_password = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, accountOrName);
                pstmt.setString(2, accountOrName);
                pstmt.setString(3, hashedPassword);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        return "SUCCESS|" + rs.getInt("User_id") + "|" + rs.getString("User_name") + "|" + rs.getString("User_account") + "|" + rs.getInt("Fish");
                    } else {
                        return !checkUserExists(conn, accountOrName) ? "ERROR|账号或用户名不存在" : "ERROR|密码错误";
                    }
                }
            }
        } catch (SQLException e) {
            return "ERROR|登录失败: " + e.getMessage();
        }
    }

    private boolean checkNameExists(Connection conn, String username) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement("SELECT 1 FROM users WHERE User_name = ? LIMIT 1")) {
            pstmt.setString(1, username);
            try (ResultSet rs = pstmt.executeQuery()) { return rs.next(); }
        }
    }

    private boolean checkAccountExists(Connection conn, String account) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement("SELECT 1 FROM users WHERE User_account = ? LIMIT 1")) {
            pstmt.setString(1, account);
            try (ResultSet rs = pstmt.executeQuery()) { return rs.next(); }
        }
    }

    private boolean checkUserExists(Connection conn, String accountOrName) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement("SELECT 1 FROM users WHERE User_account = ? OR User_name = ? LIMIT 1")) {
            pstmt.setString(1, accountOrName);
            pstmt.setString(2, accountOrName);
            try (ResultSet rs = pstmt.executeQuery()) { return rs.next(); }
        }
    }

    public String getRecords(int userId) {
        return getRecords(userId, null);
    }

    /** 按用户ID + 游戏名筛选纪录，gameName 为 null 则返回全部 */
    public String getRecords(int userId, String gameName) {
        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            String sql;
            if (gameName != null && !gameName.isEmpty()) {
                sql = "SELECT g.Game_name, g.Game_mode, r.Record FROM records r JOIN games g ON r.Game_id = g.Game_id WHERE r.User_id = ? AND g.Game_name = ?";
            } else {
                sql = "SELECT g.Game_name, g.Game_mode, r.Record FROM records r JOIN games g ON r.Game_id = g.Game_id WHERE r.User_id = ?";
            }
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, userId);
                if (gameName != null && !gameName.isEmpty()) {
                    pstmt.setString(2, gameName);
                }
                try (ResultSet rs = pstmt.executeQuery()) {
                    StringBuilder sb = new StringBuilder();
                    while (rs.next()) {
                        if (sb.length() > 0) sb.append(",");
                        sb.append(rs.getString("Game_name")).append("|").append(rs.getString("Game_mode")).append("|").append(rs.getString("Record"));
                    }
                    return "SUCCESS|" + sb.toString();
                }
            }
        } catch (SQLException e) {
            return "ERROR|读取纪录失败: " + e.getMessage();
        }
    }

    public String saveRecord(int userId, String gameName, String gameMode, String record) {
        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            int gameId = -1;
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT Game_id FROM games WHERE Game_name = ? AND Game_mode = ?")) {
                pstmt.setString(1, gameName);
                pstmt.setString(2, gameMode);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) gameId = rs.getInt("Game_id");
                    else return "ERROR|游戏不存在: " + gameName + " (" + gameMode + ")";
                }
            }
            String existing = null;
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT Record FROM records WHERE User_id = ? AND Game_id = ?")) {
                pstmt.setInt(1, userId);
                pstmt.setInt(2, gameId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) existing = rs.getString("Record");
                }
            }
            if (existing != null) {
                try (PreparedStatement pstmt = conn.prepareStatement("UPDATE records SET Record = ? WHERE User_id = ? AND Game_id = ?")) {
                    pstmt.setString(1, record);
                    pstmt.setInt(2, userId);
                    pstmt.setInt(3, gameId);
                    pstmt.executeUpdate();
                }
            } else {
                try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO records (User_id, Game_id, Record) VALUES (?, ?, ?)")) {
                    pstmt.setInt(1, userId);
                    pstmt.setInt(2, gameId);
                    pstmt.setString(3, record);
                    pstmt.executeUpdate();
                }
            }
            return "SUCCESS";
        } catch (SQLException e) {
            return "ERROR|保存纪录失败: " + e.getMessage();
        }
    }

    private String sha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(input.getBytes("UTF-8"));
            return String.format("%064x", new BigInteger(1, hash));
        } catch (Exception e) {
            return input;
        }
    }

    public String getUserState(String username) {
        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT User_state FROM users WHERE User_name = ?")) {
                pstmt.setString(1, username);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) return "SUCCESS|" + rs.getInt("User_state");
                    return "ERROR|用户不存在";
                }
            }
        } catch (SQLException e) {
            return "ERROR|查询失败: " + e.getMessage();
        }
    }

    public String setUserState(String username, int state) {
        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            try (PreparedStatement pstmt = conn.prepareStatement("UPDATE users SET User_state = ? WHERE User_name = ?")) {
                pstmt.setInt(1, state);
                pstmt.setString(2, username);
                int rows = pstmt.executeUpdate();
                return rows > 0 ? "SUCCESS" : "ERROR|用户不存在";
            }
        } catch (SQLException e) {
            return "ERROR|更新失败: " + e.getMessage();
        }
    }

    public String changePassword(String username, String oldPassword, String newPassword) {
        if (username == null || username.trim().isEmpty()) return "ERROR|用户名不能为空";
        if (oldPassword == null || oldPassword.trim().isEmpty()) return "ERROR|请输入旧密码";
        if (newPassword == null || newPassword.trim().isEmpty()) return "ERROR|请输入新密码";

        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            String oldHash = sha256(oldPassword);
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT 1 FROM users WHERE User_name = ? AND User_password = ?")) {
                pstmt.setString(1, username);
                pstmt.setString(2, oldHash);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (!rs.next()) return "ERROR|旧密码错误";
                }
            }
            try (PreparedStatement pstmt = conn.prepareStatement("UPDATE users SET User_password = ? WHERE User_name = ?")) {
                pstmt.setString(1, sha256(newPassword));
                pstmt.setString(2, username);
                int rows = pstmt.executeUpdate();
                return rows > 0 ? "SUCCESS" : "ERROR|修改失败";
            }
        } catch (SQLException e) {
            return "ERROR|修改失败: " + e.getMessage();
        }
    }

    public String addFriend(String username, String targetName) {
        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            int currentUserId = getUserIdByNameOrAccount(conn, username);
            if (currentUserId == -1) return "ERROR|当前用户不存在";
            int targetUserId = getUserIdByNameOrAccount(conn, targetName);
            if (targetUserId == -1) return "ERROR|该用户不存在";
            if (currentUserId == targetUserId) return "ERROR|不能添加自己为好友";

            // 检查是否已经是好友
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT 1 FROM friends WHERE (User_id = ? AND Friend_id = ?) OR (User_id = ? AND Friend_id = ?)")) {
                pstmt.setInt(1, currentUserId);
                pstmt.setInt(2, targetUserId);
                pstmt.setInt(3, targetUserId);
                pstmt.setInt(4, currentUserId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) return "ERROR|已经是好友了";
                }
            }
            // 添加双向好友关系
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO friends (User_id, Friend_id) VALUES (?, ?), (?, ?)")) {
                pstmt.setInt(1, currentUserId);
                pstmt.setInt(2, targetUserId);
                pstmt.setInt(3, targetUserId);
                pstmt.setInt(4, currentUserId);
                pstmt.executeUpdate();
                return "SUCCESS";
            }
        } catch (SQLException e) {
            return "ERROR|添加好友失败: " + e.getMessage();
        }
    }

    public String deleteFriend(String username, String targetName) {
        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            int currentUserId = getUserIdByNameOrAccount(conn, username);
            if (currentUserId == -1) return "ERROR|当前用户不存在";
            int targetUserId = getUserIdByNameOrAccount(conn, targetName);
            if (targetUserId == -1) return "ERROR|该用户不存在";
            // moyu官方 不允许删除
            if ("moyu官方".equals(targetName)) return "ERROR|无法删除官方好友";

            try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM friends WHERE (User_id = ? AND Friend_id = ?) OR (User_id = ? AND Friend_id = ?)")) {
                pstmt.setInt(1, currentUserId);
                pstmt.setInt(2, targetUserId);
                pstmt.setInt(3, targetUserId);
                pstmt.setInt(4, currentUserId);
                pstmt.executeUpdate();
            }
            try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM chats WHERE (Send_id = ? AND Receive_id = ?) OR (Send_id = ? AND Receive_id = ?)")) {
                pstmt.setInt(1, currentUserId);
                pstmt.setInt(2, targetUserId);
                pstmt.setInt(3, targetUserId);
                pstmt.setInt(4, currentUserId);
                pstmt.executeUpdate();
                return "SUCCESS";
            }
        } catch (SQLException e) {
            return "ERROR|删除好友失败: " + e.getMessage();
        }
    }

    public String getFriends(String username) {
        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            int currentUserId = getUserIdByNameOrAccount(conn, username);
            if (currentUserId == -1) return "ERROR|当前用户不存在";

            String sql = "SELECT DISTINCT u.User_name, u.User_state "
                       + "FROM friends f JOIN users u ON (f.User_id = u.User_id AND f.Friend_id = ?) OR (f.Friend_id = u.User_id AND f.User_id = ?) "
                       + "ORDER BY u.User_name";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, currentUserId);
                pstmt.setInt(2, currentUserId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    StringBuilder sb = new StringBuilder();
                    while (rs.next()) {
                        if (sb.length() > 0) sb.append(";");
                        sb.append(rs.getString("User_name")).append(",").append(rs.getInt("User_state"));
                    }
                    return "SUCCESS|" + sb.toString();
                }
            }
        } catch (SQLException e) {
            return "ERROR|查询好友列表失败: " + e.getMessage();
        }
    }

    public String getMessages(String username) {
        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            int userId = getUserIdByNameOrAccount(conn, username);
            if (userId == -1) return "ERROR|用户不存在";

            String sql = "SELECT c.Chat_message, c.Chat_state, c.Chat_time, s.User_name AS sender, r.User_name AS receiver "
                       + "FROM chats c JOIN users s ON c.Send_id = s.User_id JOIN users r ON c.Receive_id = r.User_id "
                       + "WHERE c.Send_id = ? OR c.Receive_id = ? ORDER BY c.Chat_time ASC LIMIT 200";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, userId);
                pstmt.setInt(2, userId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    StringBuilder sb = new StringBuilder();
                    while (rs.next()) {
                        if (sb.length() > 0) sb.append(";");
                        sb.append(rs.getString("sender")).append("|").append(rs.getString("receiver")).append("|").append(rs.getString("Chat_message")).append("|").append(rs.getInt("Chat_state")).append("|").append(rs.getTimestamp("Chat_time"));
                    }
                    return "SUCCESS|" + sb.toString();
                }
            }
        } catch (SQLException e) {
            return "ERROR|查询消息失败: " + e.getMessage();
        }
    }

    public String sendMessage(String sender, String receiver, String message, int chatState) {
        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            int senderId = getUserIdByNameOrAccount(conn, sender);
            if (senderId == -1) return "ERROR|发送者不存在";
            int receiverId = getUserIdByNameOrAccount(conn, receiver);
            if (receiverId == -1) return "ERROR|接收者不存在";

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO chats (Send_id, Receive_id, Chat_message, Chat_state) VALUES (?, ?, ?, 0)")) {
                pstmt.setInt(1, senderId);
                pstmt.setInt(2, receiverId);
                pstmt.setString(3, message);
                pstmt.executeUpdate();
                return "SUCCESS";
            }
        } catch (SQLException e) {
            return "ERROR|发送消息失败: " + e.getMessage();
        }
    }

    public String getUnreadCount(String username) {
        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            int userId = getUserIdByNameOrAccount(conn, username);
            if (userId == -1) return "ERROR|用户不存在";
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT COUNT(*) as cnt FROM chats WHERE Receive_id = ? AND Chat_state = 0")) {
                pstmt.setInt(1, userId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) return "SUCCESS|" + rs.getInt("cnt");
                    return "SUCCESS|0";
                }
            }
        } catch (SQLException e) {
            return "ERROR|查询失败: " + e.getMessage();
        }
    }

    public String markAllRead(String username) {
        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            int userId = getUserIdByNameOrAccount(conn, username);
            if (userId == -1) return "ERROR|用户不存在";
            try (PreparedStatement pstmt = conn.prepareStatement("UPDATE chats SET Chat_state = 1 WHERE Receive_id = ?")) {
                pstmt.setInt(1, userId);
                pstmt.executeUpdate();
                return "SUCCESS";
            }
        } catch (SQLException e) {
            return "ERROR|标记失败: " + e.getMessage();
        }
    }

    public String getRecentChat(String username, String friendName, int limit) {
        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            int userId = getUserIdByNameOrAccount(conn, username);
            if (userId == -1) return "ERROR|用户不存在";
            int friendId = getUserIdByNameOrAccount(conn, friendName);
            if (friendId == -1) return "ERROR|好友不存在";

            String sql = "SELECT c.Chat_message, c.Chat_state, c.Chat_time, s.User_name AS sender, r.User_name AS receiver "
                       + "FROM chats c JOIN users s ON c.Send_id = s.User_id JOIN users r ON c.Receive_id = r.User_id "
                       + "WHERE ((c.Send_id = ? AND c.Receive_id = ?) OR (c.Send_id = ? AND c.Receive_id = ?)) "
                       + "ORDER BY c.Chat_time DESC LIMIT ?";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, userId);
                pstmt.setInt(2, friendId);
                pstmt.setInt(3, friendId);
                pstmt.setInt(4, userId);
                pstmt.setInt(5, limit);
                try (ResultSet rs = pstmt.executeQuery()) {
                    StringBuilder sb = new StringBuilder();
                    while (rs.next()) {
                        if (sb.length() > 0) sb.append(";");
                        sb.append(rs.getString("sender"))
                          .append("|").append(rs.getString("receiver"))
                          .append("|").append(rs.getString("Chat_message"))
                          .append("|").append(rs.getInt("Chat_state")).append("|").append(rs.getTimestamp("Chat_time"));
                    }
                    return "SUCCESS|" + sb.toString();
                }
            }
        } catch (SQLException e) {
            return "ERROR|查询聊天失败: " + e.getMessage();
        }
    }

    /** 有未读消息时按顺序返回所有未读，否则返回最近N条 */
    public String getRecentChatWithUnread(String username, String friendName, int limit) {
        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            int userId = getUserIdByNameOrAccount(conn, username);
            if (userId == -1) return "ERROR|用户不存在";
            int friendId = getUserIdByNameOrAccount(conn, friendName);
            if (friendId == -1) return "ERROR|好友不存在";

            // 先查是否有未读（包含双向）
            int unreadCount = 0;
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "SELECT COUNT(*) as cnt FROM chats WHERE Chat_state = 0 "
                  + "AND ((Send_id = ? AND Receive_id = ?) OR (Send_id = ? AND Receive_id = ?))")) {
                pstmt.setInt(1, userId);
                pstmt.setInt(2, friendId);
                pstmt.setInt(3, friendId);
                pstmt.setInt(4, userId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) unreadCount = rs.getInt("cnt");
                }
            }

            String sql;
            PreparedStatement pstmt;
            if (unreadCount > 0) {
                // 有未读：返回所有聊天记录（包含已读和未读），按时间正序
                sql = "SELECT c.Chat_message, c.Chat_state, c.Chat_time, s.User_name AS sender, r.User_name AS receiver "
                    + "FROM chats c JOIN users s ON c.Send_id = s.User_id JOIN users r ON c.Receive_id = r.User_id "
                    + "WHERE ((c.Send_id = ? AND c.Receive_id = ?) OR (c.Send_id = ? AND c.Receive_id = ?)) "
                    + "ORDER BY c.Chat_time ASC";
                pstmt = conn.prepareStatement(sql);
                pstmt.setInt(1, userId);
                pstmt.setInt(2, friendId);
                pstmt.setInt(3, friendId);
                pstmt.setInt(4, userId);
            } else {
                // 无未读：返回最近N条，按时间正序（旧→新）
                sql = "SELECT c.Chat_message, c.Chat_state, c.Chat_time, s.User_name AS sender, r.User_name AS receiver "
                    + "FROM chats c JOIN users s ON c.Send_id = s.User_id JOIN users r ON c.Receive_id = r.User_id "
                    + "WHERE ((c.Send_id = ? AND c.Receive_id = ?) OR (c.Send_id = ? AND c.Receive_id = ?)) "
                    + "ORDER BY c.Chat_time ASC LIMIT ?";
                pstmt = conn.prepareStatement(sql);
                pstmt.setInt(1, userId);
                pstmt.setInt(2, friendId);
                pstmt.setInt(3, friendId);
                pstmt.setInt(4, userId);
                pstmt.setInt(5, limit);
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                StringBuilder sb = new StringBuilder();
                while (rs.next()) {
                    if (sb.length() > 0) sb.append(";");
                    sb.append(rs.getString("sender"))
                      .append("|").append(rs.getString("receiver"))
                      .append("|").append(rs.getString("Chat_message"))
                      .append("|").append(rs.getInt("Chat_state")).append("|").append(rs.getTimestamp("Chat_time"));
                }
                return "SUCCESS|" + sb.toString();
            }
        } catch (SQLException e) {
            return "ERROR|查询聊天失败: " + e.getMessage();
        }
    }

    /** 公开方法：通���用户名查询用户ID */
    public String getUserId(String username) {
        try (Connection conn = getConnection()) {
            if (conn == null) return "ERROR|数据库连接失败";
            int id = getUserIdByNameOrAccount(conn, username);
            if (id == -1) return "ERROR|用户不存在";
            return "SUCCESS|" + id;
        } catch (SQLException e) {
            return "ERROR|查询失败: " + e.getMessage();
        }
    }

    private int getUserIdByNameOrAccount(Connection conn, String nameOrAccount) {
        try (PreparedStatement pstmt = conn.prepareStatement("SELECT User_id FROM users WHERE User_name = ? OR User_account = ?")) {
            pstmt.setString(1, nameOrAccount);
            pstmt.setString(2, nameOrAccount);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) return rs.getInt("User_id");
            }
        } catch (SQLException e) {
            System.err.println("查询用户ID失败: " + e.getMessage());
        }
        return -1;
    }
}
