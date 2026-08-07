import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/** UNO 服务端权威引擎联调测试：建房 → 加机器人 → 准备 → 开局 → 出牌/摸牌 → 结束 */
public class UnoTest {
    static String HOST = "127.0.0.1";
    static int PORT = 8899;
    static int decideToggle = 0; // 交替测试"打出/过牌"两条决策分支
    static int playDecisions = 0; // 统计"摸到的牌打出"次数
    static int passDecisions = 0; // 统计"摸到的牌过牌"次数
    static final BlockingQueue<String> pushQ = new LinkedBlockingQueue<>();

    static String req(String msg) throws IOException {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(HOST, PORT), 5000);
            s.setSoTimeout(8000);
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            out.println(msg);
            String r = in.readLine();
            System.out.println("  >> " + msg);
            System.out.println("  << " + r);
            return r == null ? "" : r;
        }
    }

    static void startPush(String user) {
        Thread t = new Thread(() -> {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(HOST, PORT), 5000);
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                out.println("PUSH_REGISTER|" + user);
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.equals("PONG") || line.equals("PUSH_OK")) continue;
                    pushQ.offer(line);
                }
            } catch (Exception e) { System.out.println("[push] " + user + " 断: " + e); }
        });
        t.setDaemon(true);
        t.start();
    }

    /** 等待某类推送 */
    static String waitPush(String prefix, int sec) throws Exception {
        long dl = System.currentTimeMillis() + sec * 1000L;
        List<String> seen = new ArrayList<>();
        while (System.currentTimeMillis() < dl) {
            String m = pushQ.poll(500, TimeUnit.MILLISECONDS);
            if (m == null) continue;
            seen.add(m);
            if (m.contains(prefix)) return m;
        }
        System.out.println("  !! 等 " + prefix + " 超时，期间收到: " + seen);
        return null;
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0) PORT = Integer.parseInt(args[0]);
        String me = "测试甲";
        System.out.println("=== 1. 注册推送 ===");
        startPush(me);
        Thread.sleep(600);

        System.out.println("=== 2. 建 UNO 房（4人，普通叠加 mode=0）===");
        String r = req("DUEL_CREATE|" + me + "|0|4|UNO");
        if (!r.startsWith("SUCCESS")) { System.out.println("建房失败，终止"); return; }
        int roomId = Integer.parseInt(r.split("\\|")[1]);
        System.out.println("  房间号 = " + roomId);

        System.out.println("=== 2.5 房主改玩法模式（0 -> 1 -> 0）===");
        req("DUEL_UPDATE_MODE|" + roomId + "|" + me + "|1");
        req("DUEL_UPDATE_MODE|" + roomId + "|" + me + "|0");
        req("DUEL_UPDATE_MODE|" + roomId + "|路人乙|1"); // 应报错：非房主

        System.out.println("=== 3. 加 3 个机器人 ===");
        for (int i = 0; i < 3; i++) req("DUEL_ADD_BOT|" + roomId + "|" + me);

        System.out.println("=== 4. 房间信息 ===");
        req("DUEL_INFO|" + roomId);

        System.out.println("=== 5. 准备（应触发开局）===");
        req("DUEL_READY|" + roomId + "|" + me);

        String gs = waitPush("DUEL_GAME_START:", 8);
        System.out.println("  GAME_START 推送: " + gs);
        // 检查是否重复推送 GAME_START（之前 UNO 分支推了两次）
        int dupStart = 0;
        List<String> buffered = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            String m = pushQ.poll(300, TimeUnit.MILLISECONDS);
            if (m == null) break;
            if (m.contains("DUEL_GAME_START:")) dupStart++;
            buffered.add(m);
        }
        pushQ.addAll(buffered);
        System.out.println("  额外的 GAME_START 条数（期望 0）= " + dupStart);

        String st = waitPush("UNO_STATE:", 8);
        System.out.println("  首个 UNO_STATE: " + st);
        if (st == null) { System.out.println("!!! 没收到 UNO_STATE，引擎未启动"); return; }

        // 解析状态
        System.out.println("=== 6. 解析状态并尝试出牌 ===");
        for (int turn = 0; turn < 60; turn++) {
            String raw = req("UNO_STATE|" + roomId + "|" + me);
            if (!raw.startsWith("SUCCESS|")) { System.out.println("  状态查询失败: " + raw); break; }
            String body = raw.substring("SUCCESS|".length());
            String[] f = body.split("\\|");
            int curColor = Integer.parseInt(f[1]);
            int curIdx = Integer.parseInt(f[2]);
            int pending = Integer.parseInt(f[4]);
            String top = f[6];
            boolean over = "1".equals(f[7]);
            String winner = f[8];
            String chal = f[10];
            // 注意：序列化字段顺序已扩展，drawnDecide 在 [11]，玩家条目从 [12] 开始
            String drawnDecide = (f.length > 11) ? f[11] : "";
            System.out.println("  色=" + "RYGB".charAt(curColor) + " 轮到idx=" + curIdx
                    + " 顶牌=" + top + " 待摸=" + pending + " over=" + over + " winner=" + winner
                    + " 待质疑=" + chal + " 摸牌决策=" + drawnDecide);
            if (over) { System.out.println("  >>> 对局结束，赢家 = " + winner); break; }

            // 找 MYHAND（玩家条目从下标 12 开始；11 是 drawnDecide）
            int mh = -1;
            for (int i = 12; i < f.length; i++) if ("MYHAND".equals(f[i])) { mh = i; break; }
            List<String> names = new ArrayList<>();
            for (int i = 12; i < (mh < 0 ? f.length : mh); i++) names.add(f[i]);
            System.out.println("  玩家: " + names);
            String hand = (mh >= 0 && mh + 1 < f.length) ? f[mh + 1] : "";
            System.out.println("  我的手牌: " + hand);

            // 是否轮到我
            int myIdx = -1;
            for (int i = 0; i < names.size(); i++) if (names.get(i).startsWith(me + ",")) myIdx = i;
            if (chal != null && chal.equals(me)) {
                System.out.println("  -> 我需要决定质疑，选择不质疑");
                req("UNO_CHALLENGE|" + roomId + "|" + me + "|0");
                Thread.sleep(400);
                continue;
            }
            if (myIdx != curIdx) {
                System.out.println("  -> 不是我的回合，等机器人");
                Thread.sleep(1200);
                continue;
            }

            // 摸到的牌恰好能出 → 必须先用 UNO_DRAW_DECIDE 决策（交替测试 打出/过牌）
            if (drawnDecide != null && drawnDecide.startsWith(me + ",")) {
                decideToggle++;
                boolean play = (decideToggle % 2 == 1); // 第 1、3、5…次打出，第 2、4…次过牌
                String idxPart = drawnDecide.substring((me + ",").length()).trim();
                int sIdx = Integer.parseInt(idxPart);
                String[] cardsNow = hand.isEmpty() ? new String[0] : hand.split(",");
                String colorArg = "x";
                if (sIdx >= 0 && sIdx < cardsNow.length && cardsNow[sIdx].charAt(0) == 'K') colorArg = "R";
                System.out.println("  -> 摸到的牌可出(手牌idx=" + sIdx + " " + (sIdx < cardsNow.length ? cardsNow[sIdx] : "?")
                        + ")，决策: " + (play ? "打出" : "过牌") + " 选色=" + colorArg);
                String dr = req("UNO_DRAW_DECIDE|" + roomId + "|" + me + "|" + (play ? "1" : "0") + "|" + colorArg);
                if (!dr.startsWith("SUCCESS")) System.out.println("  决策被拒: " + dr);
                if (play) playDecisions++; else passDecisions++;
                Thread.sleep(500);
                continue;
            }

            // 主动摸 1 张：稳定覆盖"摸到的牌恰好能出 → 决策"路径（pending>0 时是罚摸，不触发决策）
            if (pending == 0) {
                System.out.println("  -> 主动摸 1 张（覆盖 draw-decide 路径）");
            } else {
                System.out.println("  -> 被加牌，认罚摸牌（罚摸不触发决策）");
            }
            req("UNO_DRAW|" + roomId + "|" + me);
            Thread.sleep(400);
            continue;
        }

        System.out.println("=== 7. 收尾：读残余推送 ===");
        for (int i = 0; i < 8; i++) {
            String m = pushQ.poll(400, TimeUnit.MILLISECONDS);
            if (m == null) break;
            System.out.println("  push: " + (m.length() > 200 ? m.substring(0, 200) + "..." : m));
        }
        System.out.println("=== 摸牌决策覆盖统计 ===");
        System.out.println("  打出(play=1) 次数 = " + playDecisions);
        System.out.println("  过牌(play=0) 次数 = " + passDecisions);
        if (playDecisions == 0 || passDecisions == 0)
            System.out.println("  !! 警告：两条决策分支未都覆盖（随机对局所致，可重跑）");
        else
            System.out.println("  OK：打出与过牌两条分支均已覆盖");
        System.out.println("=== 测试完成 ===");
    }

    static boolean canPlay(String c, String top, int activeColor, int pending) {
        int color = "RYGBK".indexOf(c.charAt(0));
        char tc = c.charAt(1);
        int type = (tc >= '0' && tc <= '9') ? (tc - '0')
                : (tc == 's' ? 10 : tc == 'r' ? 11 : tc == 'd' ? 12 : tc == 'w' ? 13 : 14);
        int tcolor = "RYGBK".indexOf(top.charAt(0));
        char ttc = top.charAt(1);
        int ttype = (ttc >= '0' && ttc <= '9') ? (ttc - '0')
                : (ttc == 's' ? 10 : ttc == 'r' ? 11 : ttc == 'd' ? 12 : ttc == 'w' ? 13 : 14);
        boolean wild = (type == 13 || type == 14);
        if (pending > 0) {
            if (type == 12 && ttype == 12) return true;
            if (type == 14 && (ttype == 12 || ttype == 14)) return true;
            return false;
        }
        if (wild) return true;
        if (color == activeColor) return true;
        if (type <= 9 && ttype <= 9 && c.substring(2).equals(top.substring(2))) return true;
        if (type >= 10 && !wild && type == ttype) return true;
        return false;
    }
}
