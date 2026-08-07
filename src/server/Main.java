package server;

import server.db.DatabaseManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    private static final int PORT = Integer.getInteger("server.port", 80);
    private static DatabaseManager dbManager;
    private static final Map<String, PushSession> pushClients = new ConcurrentHashMap<>();
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(20);
    private static final Semaphore connLimit = new Semaphore(50);
    private static final AtomicInteger activeConnections = new AtomicInteger(0);

    static class DuelRoom {
        int roomId;
        String creator;
        String mode;
        int maxPlayers;
        String gameType;
        Map<String, Boolean> players = new LinkedHashMap<>(); // username -> isReady

        // 游戏状态
        boolean gameStarted = false;
        long gameSeed = 0;
        // 玩家游戏结果: username -> "WIN:time" 或 "FAIL:time"
        Map<String, String> gameResults = new LinkedHashMap<>();

        // 2048 机器人状态
        Map<String, Bot2048State> bot2048States = new HashMap<>();

        // UNO 权威对局状态（gameType==UNO 时由服务端持有）
        UnoMatch uno = null;

        DuelRoom(int roomId, String creator, String mode, int maxPlayers, String gameType) {
            this.roomId = roomId;
            this.creator = creator;
            this.mode = mode;
            this.maxPlayers = maxPlayers;
            this.gameType = gameType;
            this.players.put(creator, false);
        }

        /** 启动 2048 机器人 AI */
        synchronized void start2048Bots(ScheduledExecutorService scheduler) {
            if (!"2048".equals(gameType)) return;
            bot2048States.clear();
            for (String p : players.keySet()) {
                if (p.startsWith("机器人")) {
                    Bot2048State bot = new Bot2048State(p, gameSeed ^ p.hashCode());
                    bot2048States.put(p, bot);
                    bot.start(scheduler, this);
                }
            }
        }

        /** 停止 2048 机器人 AI */
        synchronized void stop2048Bots() {
            for (Bot2048State bot : bot2048States.values()) {
                bot.stop();
            }
            bot2048States.clear();
        }

        synchronized String joinRoom(String username) {
            if (players.containsKey(username)) return "SUCCESS|" + getState();
            if (players.size() >= maxPlayers) return "ERROR|房间已满";
            players.put(username, false);
            return "SUCCESS|" + getState();
        }

        synchronized String leaveRoom(String username) {
            players.remove(username);
            return "SUCCESS|" + getState();
        }

        synchronized String toggleReady(String username) {
            if (!players.containsKey(username)) return "ERROR|你不在房间中";
            players.put(username, !players.get(username));
            boolean allReady = true;
            for (Boolean r : players.values()) { if (!r) { allReady = false; break; } }
            return "SUCCESS|" + getState() + (allReady && players.size() == maxPlayers ? "|ALL_READY" : "");
        }

        synchronized String setMaxPlayers(String requester, int newMax) {
            if (!requester.equals(creator)) return "ERROR|只有房主可以修改人数";
            if (newMax < players.size()) return "ERROR|对决人数不能少于当前玩家数(" + players.size() + "人)";
            int maxCap = "UNO".equals(gameType) ? 8 : 4;
            if (newMax < 2 || newMax > maxCap) return "ERROR|人数必须在2-" + maxCap + "之间";
            this.maxPlayers = newMax;
            return "SUCCESS|" + getState();
        }

        /** 记录玩家游戏结果 */
        synchronized String recordGameResult(String username, String result, long time, int score) {
            if (!players.containsKey(username)) return "ERROR|玩家不在房间中";
            gameResults.put(username, result + ":" + time + ":" + score);
            // 为所有机器人自动填充结果，确保能触发 ALL_DONE
            for (String p : players.keySet()) {
                if (p.startsWith("机器人") && !gameResults.containsKey(p)) {
                    Bot2048State bot = bot2048States.get(p);
                    int botScore = bot != null ? bot.score : 0;
                    gameResults.put(p, "FAIL:" + time + ":" + botScore);
                }
            }
            // 检查是否所有人都出结果了
            if (gameResults.size() >= players.size()) {
                stop2048Bots();
                return "SUCCESS|ALL_DONE|" + getGameOverData();
            }
            return "SUCCESS|WAITING";
        }

        /** 序列化游戏结果数据 */
        String getGameOverData() {
            // 若全部人都 FAIL，按分数高低决定赢家（分数为0时回退到存活时间）
            boolean hasWin = false;
            for (String v : gameResults.values()) {
                if (v.startsWith("WIN:")) { hasWin = true; break; }
            }
            if (!hasWin && !gameResults.isEmpty()) {
                String winner = null;
                long winTime = 0;
                int winScore = -1;
                boolean anyScore = false;
                String lastSurvivor = null;
                long maxTime = -1;
                for (Map.Entry<String, String> e : gameResults.entrySet()) {
                    String[] parts = e.getValue().split(":");
                    long t = 0;
                    int sc = 0;
                    if (parts.length >= 2) {
                        try { t = Long.parseLong(parts[1]); } catch (NumberFormatException ignored) {}
                    }
                    if (parts.length >= 3) {
                        try { sc = Integer.parseInt(parts[2]); } catch (NumberFormatException ignored) {}
                    }
                    if (sc > 0) anyScore = true;
                    if (sc > winScore) { winScore = sc; winner = e.getKey(); winTime = t; }
                    if (t > maxTime) { maxTime = t; lastSurvivor = e.getKey(); }
                }
                if (anyScore && winner != null) {
                    // 有分数：最高分赢
                    gameResults.put(winner, "WIN:" + winTime + ":" + winScore);
                } else if (lastSurvivor != null) {
                    // 无分数（扫雷等）：存活最久赢
                    gameResults.put(lastSurvivor, "WIN:" + maxTime + ":" + 0);
                }
            }

            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> e : gameResults.entrySet()) {
                if (sb.length() > 0) sb.append(";");
                sb.append(e.getKey()).append(",").append(e.getValue());
            }
            return sb.toString();
        }

        String getState() {
            StringBuilder sb = new StringBuilder();
            sb.append(roomId).append("|").append(mode).append("|").append(maxPlayers).append("|").append(gameType);
            for (Map.Entry<String, Boolean> e : players.entrySet()) {
                sb.append("|").append(e.getKey()).append(",").append(e.getValue() ? "1" : "0");
            }
            return sb.toString();
        }
    }

    /** 2048 机器人状态与简单 AI */
    static class Bot2048State {
        String name;
        int[][] board = new int[4][4];
        int score = 0;
        Random rnd;
        volatile boolean finished = false;
        long startTime;
        ScheduledFuture<?> task;
        static final int SIZE = 4;

        Bot2048State(String name, long seed) {
            this.name = name;
            this.rnd = new Random(seed);
            this.startTime = System.currentTimeMillis();
            addRandomTile();
            addRandomTile();
        }

        void start(ScheduledExecutorService scheduler, DuelRoom room) {
            broadcastBoard(room);
            task = scheduler.scheduleAtFixedRate(() -> {
                if (finished) return;
                makeMove(room);
            }, 800 + rnd.nextInt(800), 600 + rnd.nextInt(600), TimeUnit.MILLISECONDS);
        }

        void stop() {
            finished = true;
            if (task != null) task.cancel(false);
        }

        void makeMove(DuelRoom room) {
            List<Integer> dirs = new ArrayList<>(Arrays.asList(0, 1, 2, 3));
            Collections.shuffle(dirs, rnd);
            boolean moved = false;
            for (int dir : dirs) {
                if (tryMove(dir)) {
                    moved = true;
                    break;
                }
            }
            if (moved) {
                addRandomTile();
                broadcastBoard(room);
            }
            if (!moved || (isFull() && !canMove())) {
                finish(room);
            }
        }

        void finish(DuelRoom room) {
            if (finished) return;
            finished = true;
            if (task != null) task.cancel(false);
            long time = System.currentTimeMillis() - startTime;
            // 异步提交结果，避免阻塞调度线程
            new Thread(() -> {
                String res = processRequest("DUEL_GAME_RESULT|" + room.roomId + "|" + name + "|FAIL|" + time + "|" + score);
                System.out.println("[2048机器人] " + name + " 结束，分数=" + score + "，结果=" + res);
            }).start();
        }

        void broadcastBoard(DuelRoom room) {
            String boardStr = serializeBoard();
            String push = "DUEL_BOARD_PUSH:" + room.roomId + ":" + name + ":" + score + ":" + boardStr;
            for (String p : room.players.keySet()) {
                if (!p.equals(name)) {
                    pushToUser(p, "系统", push);
                }
            }
        }

        String serializeBoard() {
            StringBuilder sb = new StringBuilder();
            for (int r = 0; r < SIZE; r++) {
                for (int c = 0; c < SIZE; c++) {
                    if (sb.length() > 0) sb.append(",");
                    sb.append(board[r][c]);
                }
            }
            return sb.toString();
        }

        boolean tryMove(int dir) {
            int[][] prev = new int[SIZE][SIZE];
            for (int r = 0; r < SIZE; r++) System.arraycopy(board[r], 0, prev[r], 0, SIZE);
            if (dir == 0) moveLeft();
            else if (dir == 1) moveRight();
            else if (dir == 2) moveUp();
            else if (dir == 3) moveDown();
            for (int r = 0; r < SIZE; r++) {
                for (int c = 0; c < SIZE; c++) {
                    if (board[r][c] != prev[r][c]) return true;
                }
            }
            return false;
        }

        void moveLeft() {
            for (int r = 0; r < SIZE; r++) board[r] = compress(board[r]);
        }
        void moveRight() {
            for (int r = 0; r < SIZE; r++) {
                reverse(board[r]);
                board[r] = compress(board[r]);
                reverse(board[r]);
            }
        }
        void moveUp() {
            for (int c = 0; c < SIZE; c++) {
                int[] col = new int[SIZE];
                for (int r = 0; r < SIZE; r++) col[r] = board[r][c];
                col = compress(col);
                for (int r = 0; r < SIZE; r++) board[r][c] = col[r];
            }
        }
        void moveDown() {
            for (int c = 0; c < SIZE; c++) {
                int[] col = new int[SIZE];
                for (int r = 0; r < SIZE; r++) col[r] = board[r][c];
                reverse(col);
                col = compress(col);
                reverse(col);
                for (int r = 0; r < SIZE; r++) board[r][c] = col[r];
            }
        }

        int[] compress(int[] row) {
            int[] result = new int[SIZE];
            int idx = 0;
            boolean lastMerged = false;
            for (int v : row) {
                if (v == 0) continue;
                if (idx > 0 && result[idx - 1] == v && !lastMerged) {
                    result[idx - 1] *= 2;
                    score += result[idx - 1];
                    lastMerged = true;
                } else {
                    result[idx++] = v;
                    lastMerged = false;
                }
            }
            return result;
        }

        void reverse(int[] arr) {
            for (int i = 0; i < arr.length / 2; i++) {
                int tmp = arr[i];
                arr[i] = arr[arr.length - 1 - i];
                arr[arr.length - 1 - i] = tmp;
            }
        }

        void addRandomTile() {
            List<int[]> empty = new ArrayList<>();
            for (int r = 0; r < SIZE; r++)
                for (int c = 0; c < SIZE; c++)
                    if (board[r][c] == 0) empty.add(new int[]{r, c});
            if (empty.isEmpty()) return;
            int[] pos = empty.get(rnd.nextInt(empty.size()));
            board[pos[0]][pos[1]] = rnd.nextDouble() < 0.9 ? 2 : 4;
        }

        boolean isFull() {
            for (int r = 0; r < SIZE; r++)
                for (int c = 0; c < SIZE; c++)
                    if (board[r][c] == 0) return false;
            return true;
        }

        boolean canMove() {
            for (int r = 0; r < SIZE; r++)
                for (int c = 0; c < SIZE; c++) {
                    if (board[r][c] == 0) return true;
                    if (c < SIZE - 1 && board[r][c] == board[r][c + 1]) return true;
                    if (r < SIZE - 1 && board[r][c] == board[r + 1][c]) return true;
                }
            return false;
        }
    }

    // ============================================================
    //                    UNO 权威对局引擎（服务端持有全部状态）
    // ============================================================
    /** UNO 牌：color 0=R 1=Y 2=G 3=B 4=W(万能)；type 0-9=数字 10=跳过 11=反转 12=+2 13=变色 14=+4 */
    static class UnoCardS {
        int color, type, number;
        UnoCardS(int c, int t, int n) { color = c; type = t; number = n; }
        boolean isWild() { return type == 13 || type == 14; }
        boolean canPlayOn(UnoCardS top, int activeColor, int pendingDraws, boolean rev) {
            if (pendingDraws > 0) {
                if (type == 12 && top.type == 12) return true;
                if (type == 14 && (top.type == 12 || top.type == 14)) return true;
                if (rev && type == 11 && color == activeColor) return true;
                return false;
            }
            if (isWild()) return true;
            if (color == activeColor) return true;
            if (type >= 0 && type <= 9 && top.type >= 0 && top.type <= 9 && number == top.number) return true;
            if (type >= 10 && !isWild() && type == top.type) return true;
            return false;
        }
        static String enc(UnoCardS c) {
            char col = "RYGBK".charAt(c.color);
            char typ;
            if (c.type >= 0 && c.type <= 9) typ = (char) ('0' + c.type);
            else typ = new char[]{'?','?','?','?','?','?','?','?','?','?','s','r','d','w','f'}[c.type];
            return "" + col + typ + (c.type >= 0 && c.type <= 9 ? ("" + c.number) : "");
        }
        static UnoCardS dec(String s) {
            int color = "RYGBK".indexOf(s.substring(0, 1));
            char tc = s.charAt(1);
            int type = 0, num = 0;
            if (tc >= '0' && tc <= '9') { type = tc - '0'; num = Integer.parseInt(s.substring(2)); }
            else { switch (tc) {
                case 's': type = 10; break; case 'r': type = 11; break;
                case 'd': type = 12; break; case 'w': type = 13; break;
                case 'f': type = 14; break; default: type = 0;
            } }
            return new UnoCardS(color, type, num);
        }
    }

    static class UnoMatch {
        DuelRoom room;
        List<String> order = new ArrayList<>();
        Map<String, List<UnoCardS>> hands = new LinkedHashMap<>();
        List<UnoCardS> drawPile = new ArrayList<>();
        List<UnoCardS> discard = new ArrayList<>();
        int currentColor = 0;
        int currentPlayerIdx = 0;
        int direction = 1;
        int pendingDraws = 0;
        boolean reverseMirrored = false;
        UnoCardS topCard;
        int mode = 0; // 0=普通 1=逆转叠加
        Map<String, Boolean> calledUno = new HashMap<>();
        // 抓 UNO 窗口：被标记者的下家一旦出牌/摸牌，该窗口即关闭（由 advance() 维护）
        Set<String> hammerClosed = new HashSet<>();
        // 锤子窗口在 serialize() 里按 (hand=1, !calledUno, P!=当前回合玩家, 且未被关闭) 现场计算
        boolean gameOver = false;
        String winner = null;
        String pendingChallengeAgainst = null; // 需要决定是否质疑的玩家
        String pendingChallengeTarget = null;  // 出 +4 的玩家
        long challengeDeadline = 0;
        ScheduledFuture<?> botTask = null;
        // 摸到一张恰好能出的牌 → 询问该玩家是否立刻打出（与单机版行为一致）
        String drawnPlayableUser = null;
        int drawnPlayableIdx = -1;
        ScheduledFuture<?> drawDecisionTask = null;
        Random rnd = new Random();
        // 计时
        long matchStartMs = 0;       // 开局时刻（用于客户端显示总倒计时）
        long matchDurationMs = 0;     // 本局总时长（4 人 10 分钟，每多一人 +2 分钟）
        long turnDeadline = 0;       // 当前回合截止时刻（用于客户端显示出牌倒计时）
        ScheduledFuture<?> gameTimerTask = null;
        ScheduledFuture<?> turnTimerTask = null;

        UnoMatch(DuelRoom room, int mode, long seed) {
            this.room = room;
            this.mode = mode;
            order.addAll(room.players.keySet());
            // 构建 108 张牌
            List<UnoCardS> deck = new ArrayList<>();
            for (int c = 0; c < 4; c++) {
                deck.add(new UnoCardS(c, 0, 0));
                for (int n = 1; n <= 9; n++) { deck.add(new UnoCardS(c, n, n)); deck.add(new UnoCardS(c, n, n)); }
                for (int i = 0; i < 2; i++) { deck.add(new UnoCardS(c, 10, 0)); deck.add(new UnoCardS(c, 11, 0)); deck.add(new UnoCardS(c, 12, 0)); }
            }
            for (int i = 0; i < 4; i++) { deck.add(new UnoCardS(4, 13, 0)); deck.add(new UnoCardS(4, 14, 0)); }
            // 洗牌
            Collections.shuffle(deck, new Random(seed));
            // 发 7 张
            int p = 0;
            for (String name : order) {
                List<UnoCardS> h = new ArrayList<>();
                for (int k = 0; k < 7; k++) h.add(deck.remove(deck.size() - 1));
                hands.put(name, h);
                calledUno.put(name, false);
                p++;
            }
            // 翻开顶牌（不接受 +4 作起手，重翻时放回）
            do {
                topCard = deck.remove(deck.size() - 1);
                if (topCard.type == 14) deck.add(0, topCard); // +4 不允许作起手，放回重抽
            } while (topCard.type == 14 && !deck.isEmpty());
            discard.add(topCard);
            drawPile = deck;
            if (topCard.isWild()) currentColor = rnd.nextInt(4);
            else currentColor = topCard.color;
            // 起手特殊牌处理（对齐离线单机版：以 order[0] 为起手锚点）
            // 离线以「人类」为锚，单机局人类即 order[0]，语义一致
            int firstIdx;
            if (topCard.type == 12) {            // +2：下家需接 2
                pendingDraws = 2;
                firstIdx = (0 + 1) % order.size();
            } else if (topCard.type == 10) {     // SKIP：跳过 1 人
                firstIdx = (0 + 2) % order.size();
            } else if (topCard.type == 11) {     // REVERSE：方向反转，起手玩家变为上家
                direction = -1;
                firstIdx = (order.size() - 1) % order.size();
            } else {                             // 数字牌 / 变色：下家先出
                firstIdx = (0 + 1) % order.size();
            }
            currentPlayerIdx = firstIdx;
            // 游戏总时间：4 人 10 分钟，每多一人 +2 分钟（不足 4 人也按 10 分钟）
            int n = order.size();
            this.matchDurationMs = (10 + Math.max(0, n - 4) * 2) * 60_000L;
            this.matchStartMs = System.currentTimeMillis();
        }

        void advance(boolean skip) {
            int n = order.size();
            int oldIdx = currentPlayerIdx;
            String actor = order.get(oldIdx);
            // 关闭"刚行动玩家的上家"的抓窗口——X出牌=X的下家出牌 → 上家锤子消失
            int prevIdx = (oldIdx - direction + n * 1000) % n;
            String prevPlayer = order.get(prevIdx);
            hammerClosed.add(prevPlayer);
            hammerClosed.remove(actor);
            currentPlayerIdx = (oldIdx + direction * (skip ? 2 : 1) + n * 1000) % n;
            System.out.println("[uno] advance oldIdx=" + oldIdx + " actor=" + actor + " prev=" + prevPlayer
                    + " hammerClosed=" + hammerClosed + " newIdx=" + currentPlayerIdx);
        }

        void drawCards(String user, int count) {
            List<UnoCardS> h = hands.get(user);
            for (int i = 0; i < count; i++) {
                if (drawPile.isEmpty()) {
                    // 重洗弃牌堆（保留顶牌）
                    if (discard.size() > 1) {
                        List<UnoCardS> resh = new ArrayList<>(discard.subList(0, discard.size() - 1));
                        discard.clear();
                        discard.add(topCard);
                        Collections.shuffle(resh, new Random(System.currentTimeMillis() + drawPile.size()));
                        drawPile = resh;
                    }
                }
                if (drawPile.isEmpty()) break;
                h.add(drawPile.remove(drawPile.size() - 1));
            }
            calledUno.put(user, false);
        }

        int handScore(String user) {
            int s = 0;
            for (UnoCardS c : hands.get(user)) {
                if (c.type >= 0 && c.type <= 9) s += c.number;
                else if (c.type >= 10 && c.type <= 12) s += 20;
                else s += 50;
            }
            return s;
        }

        /** 序列化公开状态；myHand 为 null 时不含手牌（仅公开部分） */
        String serialize(String myUser) {
            StringBuilder sb = new StringBuilder();
            sb.append(room.roomId).append("|")
              .append(currentColor).append("|").append(currentPlayerIdx).append("|")
              .append(direction).append("|").append(pendingDraws).append("|")
              .append(reverseMirrored ? 1 : 0).append("|")
              .append(UnoCardS.enc(topCard)).append("|")
              .append(gameOver ? 1 : 0).append("|")
              .append(winner == null ? "" : winner).append("|")
              .append(mode).append("|")
              .append(pendingChallengeAgainst == null ? "" : pendingChallengeAgainst).append("|")
              // 摸到可出牌待决策：user,handIdx（空串表示无）
              .append(drawnPlayableUser == null ? "" : (drawnPlayableUser + "," + drawnPlayableIdx));
            // 每位玩家：name,手牌数,是否已喊UNO,是否允许被抓（出锤）
            // "可被抓"= 手牌=1 且 未喊 UNO 且 现在不是他的回合（窗口在轮到他时关闭）。
            String currentActor = order.get(currentPlayerIdx);
            for (String name : order) {
                boolean canCatch = hands.get(name).size() == 1
                        && !Boolean.TRUE.equals(calledUno.get(name))
                        && !name.equals(currentActor)
                        && !hammerClosed.contains(name);
                sb.append("|").append(name).append(",").append(hands.get(name).size())
                  .append(",").append(Boolean.TRUE.equals(calledUno.get(name)) ? 1 : 0)
                  .append(",").append(canCatch ? 1 : 0);
            }
            if (myUser != null) {
                sb.append("|MYHAND|");
                List<UnoCardS> h = hands.get(myUser);
                for (int i = 0; i < h.size(); i++) {
                    if (i > 0) sb.append(",");
                    sb.append(UnoCardS.enc(h.get(i)));
                }
            }
            // 计时信息（客户端显示）：开局时刻 | 总时长 | 当前回合截止时刻
            sb.append("|").append(matchStartMs).append("|").append(matchDurationMs).append("|").append(turnDeadline);
            return sb.toString();
        }

        /** 选色：手牌中占比最高的非万能色；并列时随机选一个 */
        int pickColor(List<UnoCardS> h) {
            int[] cnt = new int[4];
            for (UnoCardS c : h) if (c.color >= 0 && c.color <= 3) cnt[c.color]++;
            int max = -1;
            for (int i = 0; i < 4; i++) if (cnt[i] > max) max = cnt[i];
            List<Integer> best = new ArrayList<>();
            for (int i = 0; i < 4; i++) if (cnt[i] == max) best.add(i);
            return best.get(rnd.nextInt(best.size()));
        }

        boolean hasPlayable(String user) {
            List<UnoCardS> h = hands.get(user);
            for (UnoCardS c : h) if (c.canPlayOn(topCard, currentColor, pendingDraws, mode == 1)) return true;
            return false;
        }
    }

    // ===== UNO 指令处理（在 synchronized(room) 内调用） =====
    private static String unoPlay(DuelRoom room, String user, int handIdx, int colorChar) {
        UnoMatch m = room.uno;
        if (m == null || m.gameOver) return "ERROR|对局未开始或已结束";
        if (m.pendingChallengeAgainst != null) return "ERROR|请先处理 +4 质疑";
        // 摸牌后待决策时，本人直接点牌出也视为"决定打出"
        if (m.drawnPlayableUser != null) {
            if (!m.drawnPlayableUser.equals(user)) return "ERROR|等待对方决定刚摸到的牌";
            m.drawnPlayableUser = null;
            m.drawnPlayableIdx = -1;
            if (m.drawDecisionTask != null) { m.drawDecisionTask.cancel(false); m.drawDecisionTask = null; }
        }
        int idx = m.order.indexOf(user);
        if (idx < 0) return "ERROR|你不在对局中";
        if (idx != m.currentPlayerIdx) return "ERROR|还没轮到你";
        List<UnoCardS> h = m.hands.get(user);
        if (handIdx < 0 || handIdx >= h.size()) return "ERROR|无效的牌";
        UnoCardS card = h.get(handIdx);
        if (!card.canPlayOn(m.topCard, m.currentColor, m.pendingDraws, m.mode == 1)) return "ERROR|这张牌不能出";
        // 注意：叠加累加统一由下面 card.type 分支的 += 完成，此处不要重复加，否则 +2 接 +2 会变成 6 张
        h.remove(handIdx);
        m.discard.add(card);
        m.topCard = card;
        if (card.isWild()) m.currentColor = (colorChar >= 0 && colorChar <= 3) ? colorChar : m.pickColor(h);
        else m.currentColor = card.color;
        boolean skip = false, replay = false;
        if (card.type == 10) skip = true;
        else if (card.type == 11) { if (m.order.size() == 2) replay = true; else m.direction *= -1; }
        else if (card.type == 12) m.pendingDraws += 2;
        else if (card.type == 14) m.pendingDraws += 4;
        if (card.type == 11) m.reverseMirrored = !m.reverseMirrored;
        // 胜利判定
        if (h.isEmpty()) { unoEnd(room, user); return "SUCCESS"; }
        if (!replay) m.advance(skip);
        // +4 质疑窗口
        if (card.type == 14) {
            m.pendingChallengeAgainst = m.order.get(m.currentPlayerIdx);
            m.pendingChallengeTarget = user;
            m.challengeDeadline = System.currentTimeMillis() + TURN_LIMIT_MS;
            scheduleUnoChallengeResolve(room);
        }
        pushUnoState(room);
        scheduleUnoBotIfNeeded(room);
        if (m.pendingChallengeAgainst == null) scheduleTurnTimer(room);
        return "SUCCESS";
    }

    private static String unoDraw(DuelRoom room, String user) {
        UnoMatch m = room.uno;
        if (m == null || m.gameOver) return "ERROR|对局未开始或已结束";
        if (m.pendingChallengeAgainst != null) return "ERROR|请先处理 +4 质疑";
        if (m.drawnPlayableUser != null) return "ERROR|请先决定刚摸到的牌是否打出";
        int idx = m.order.indexOf(user);
        if (idx < 0) return "ERROR|你不在对局中";
        if (idx != m.currentPlayerIdx) return "ERROR|还没轮到你";
        boolean penalty = m.pendingDraws > 0;
        int n = penalty ? m.pendingDraws : 1;
        m.drawCards(user, n);
        m.pendingDraws = 0;

        // 非罚摸且摸到的这张恰好能出 → 给玩家一次"是否打出"的机会（与单机版一致）
        if (!penalty) {
            List<UnoCardS> h = m.hands.get(user);
            if (h != null && !h.isEmpty()) {
                int last = h.size() - 1;
                if (h.get(last).canPlayOn(m.topCard, m.currentColor, 0, m.mode == 1)) {
                    if (user.startsWith("机器人")) {
                        // 机器人不询问，直接打出
                        int cc = h.get(last).isWild() ? m.pickColor(h) : -1;
                        return unoPlay(room, user, last, cc);
                    }
                    m.drawnPlayableUser = user;
                    m.drawnPlayableIdx = last;
                    pushUnoState(room);
                    scheduleDrawDecisionResolve(room);
                    return "SUCCESS";
                }
            }
        }
        m.advance(false);
        pushUnoState(room);
        scheduleUnoBotIfNeeded(room);
        scheduleTurnTimer(room);
        return "SUCCESS";
    }

    /** 玩家对"刚摸到的可出牌"作出决定：play=true 打出，false 过牌 */
    private static String unoDrawDecide(DuelRoom room, String user, boolean play, int colorChar) {
        UnoMatch m = room.uno;
        if (m == null || m.gameOver) return "ERROR|对局未开始或已结束";
        if (m.drawnPlayableUser == null || !m.drawnPlayableUser.equals(user)) return "ERROR|当前无需你决定";
        int cardIdx = m.drawnPlayableIdx;
        m.drawnPlayableUser = null;
        m.drawnPlayableIdx = -1;
        if (m.drawDecisionTask != null) { m.drawDecisionTask.cancel(false); m.drawDecisionTask = null; }
        if (play) return unoPlay(room, user, cardIdx, colorChar);
        m.advance(false);
        pushUnoState(room);
        scheduleUnoBotIfNeeded(room);
        scheduleTurnTimer(room);
        return "SUCCESS";
    }

    /** 12 秒不决定就自动过牌，避免整桌卡死 */
    private static void scheduleDrawDecisionResolve(DuelRoom room) {
        UnoMatch m = room.uno;
        if (m == null) return;
        if (m.drawDecisionTask != null) m.drawDecisionTask.cancel(false);
        final String who = m.drawnPlayableUser;
        m.drawDecisionTask = matchScheduler.schedule(() -> {
            synchronized (room) {
                UnoMatch mm = room.uno;
                if (mm == null || mm.gameOver) return;
                if (who == null || !who.equals(mm.drawnPlayableUser)) return;
                unoDrawDecide(room, who, false, -1);
            }
        }, 12, TimeUnit.SECONDS);
    }

    private static String unoUno(DuelRoom room, String user) {
        UnoMatch m = room.uno;
        if (m == null) return "ERROR|对局未开始";
        m.calledUno.put(user, true);
        return "SUCCESS";
    }

    private static String unoCatch(DuelRoom room, String catcher, String target) {
        UnoMatch m = room.uno;
        if (m == null || m.gameOver) return "ERROR|对局未开始或已结束";
        List<UnoCardS> th = m.hands.get(target);
        if (th == null) return "ERROR|目标不存在";
        if (th.size() != 1) return "ERROR|对方手牌不是 1 张，无法抓";
        if (Boolean.TRUE.equals(m.calledUno.get(target))) return "ERROR|对方已喊 UNO";
        m.drawCards(target, 2);
        m.calledUno.put(target, true);
        m.hammerClosed.remove(target);
        pushUnoState(room);
        scheduleUnoBotIfNeeded(room);
        return "SUCCESS";
    }

    private static String unoChallenge(DuelRoom room, String user, boolean accept) {
        UnoMatch m = room.uno;
        if (m == null || m.gameOver) return "ERROR|对局未开始或已结束";
        if (m.pendingChallengeAgainst == null || !m.pendingChallengeAgainst.equals(user)) return "ERROR|当前无需你质疑";
        String target = m.pendingChallengeTarget;
        // 出 +4 者是否本可出其他牌（非 +4 且可出）
        boolean illegal = false;
        for (UnoCardS c : m.hands.get(target)) {
            if (c.type != 14 && c.canPlayOn(m.topCard, m.currentColor, 0, m.mode == 1)) { illegal = true; break; }
        }
        m.pendingChallengeAgainst = null;
        m.pendingChallengeTarget = null;
        if (accept && illegal) m.drawCards(target, m.pendingDraws > 0 ? m.pendingDraws : 4);
        else if (accept && !illegal) m.drawCards(user, 6);
        else         m.drawCards(user, m.pendingDraws > 0 ? m.pendingDraws : 4);
        m.pendingDraws = 0;
        m.advance(false);
        pushUnoState(room);
        scheduleUnoBotIfNeeded(room);
        scheduleTurnTimer(room);
        return "SUCCESS";
    }

    private static void scheduleUnoChallengeResolve(DuelRoom room) {
        final UnoMatch m = room.uno;
        if (m == null) return;
        matchScheduler.schedule(() -> {
            synchronized (room) {
                if (room.uno != m || m.gameOver) return;
                if (m.pendingChallengeAgainst == null) return;
                String user = m.pendingChallengeAgainst;
                m.pendingChallengeAgainst = null;
                m.pendingChallengeTarget = null;
                m.drawCards(user, m.pendingDraws > 0 ? m.pendingDraws : 4);
                m.pendingDraws = 0;
                m.advance(false);
                pushUnoState(room);
                scheduleUnoBotIfNeeded(room);
                scheduleTurnTimer(room);
            }
        }, TURN_LIMIT_MS, TimeUnit.MILLISECONDS);
    }

    private static void scheduleUnoBotIfNeeded(DuelRoom room) {
        final UnoMatch m = room.uno;
        if (m == null || m.gameOver || m.pendingChallengeAgainst != null) return;
        String cur = m.order.get(m.currentPlayerIdx);
        if (!cur.startsWith("机器人")) return;
        if (m.botTask != null) m.botTask.cancel(false);
        m.botTask = matchScheduler.schedule(() -> {
            synchronized (room) {
                if (room.uno != m || m.gameOver) return;
                unoBotMove(room);
            }
        }, 1200 + m.rnd.nextInt(1200), TimeUnit.MILLISECONDS);
    }

    private static void unoBotMove(DuelRoom room) {
        UnoMatch m = room.uno;
        if (m == null || m.gameOver) return;
        String cur = m.order.get(m.currentPlayerIdx);
        if (!cur.startsWith("机器人")) return;
        // 处理质疑窗口
        if (m.pendingChallengeAgainst != null && m.pendingChallengeAgainst.equals(cur)) {
            String target = m.pendingChallengeTarget;
            boolean illegal = false;
            for (UnoCardS c : m.hands.get(target)) {
                if (c.type != 14 && c.canPlayOn(m.topCard, m.currentColor, 0, m.mode == 1)) { illegal = true; break; }
            }
            // 70% 概率在「可能违规」时质疑
            boolean accept = illegal && m.rnd.nextDouble() < 0.7;
            unoChallenge(room, cur, accept);
            return;
        }
        List<UnoCardS> h = m.hands.get(cur);
        // 先找普通牌，找不到再用万能牌，避免一上来就把 +4 / 变色浪费掉
        int playIdx = -1;
        for (int i = 0; i < h.size(); i++) {
            UnoCardS c = h.get(i);
            if (!c.isWild() && c.canPlayOn(m.topCard, m.currentColor, m.pendingDraws, m.mode == 1)) { playIdx = i; break; }
        }
        if (playIdx < 0) {
            for (int i = 0; i < h.size(); i++) {
                if (h.get(i).canPlayOn(m.topCard, m.currentColor, m.pendingDraws, m.mode == 1)) { playIdx = i; break; }
            }
        }
        if (playIdx >= 0) {
            UnoCardS card = h.get(playIdx);
            int colorChar = card.isWild() ? m.pickColor(h) : -1;
            // 机器人有 70% 概率记得喊 UNO，剩下 30% 会被玩家抓（保留可玩性）
            if (h.size() == 2 && m.rnd.nextDouble() < 0.7) m.calledUno.put(cur, true);
            unoPlay(room, cur, playIdx, colorChar);
        } else {
            unoDraw(room, cur);
        }
    }

    /** 回合限时：当前玩家 TURN_LIMIT_MS 内未行动，则由系统代打 / 代摸 */
    private static void scheduleTurnTimer(DuelRoom room) {
        UnoMatch m = room.uno;
        if (m == null || m.gameOver) return;
        if (m.pendingChallengeAgainst != null || m.drawnPlayableUser != null) return;
        if (m.turnTimerTask != null) m.turnTimerTask.cancel(false);
        final String cur = m.order.get(m.currentPlayerIdx);
        m.turnDeadline = System.currentTimeMillis() + TURN_LIMIT_MS;
        m.turnTimerTask = matchScheduler.schedule(() -> {
            synchronized (room) {
                if (room.uno != m || m.gameOver) return;
                if (m.pendingChallengeAgainst != null || m.drawnPlayableUser != null) return;
                if (!m.order.get(m.currentPlayerIdx).equals(cur)) return; // 已有人行动，作废
                autoPlayOrDraw(room, cur);
            }
        }, TURN_LIMIT_MS, TimeUnit.MILLISECONDS);
    }

    /** 系统代打：打出一张能出的牌；无可出则摸牌，摸到的能出就直接打出 */
    private static void autoPlayOrDraw(DuelRoom room, String user) {
        UnoMatch m = room.uno;
        if (m == null || m.gameOver) return;
        List<UnoCardS> h = m.hands.get(user);
        if (h == null) return;
        // 1) 先找普通可出牌，再退而求其次用万能牌
        int playIdx = -1;
        for (int i = 0; i < h.size(); i++) {
            UnoCardS c = h.get(i);
            if (!c.isWild() && c.canPlayOn(m.topCard, m.currentColor, m.pendingDraws, m.mode == 1)) { playIdx = i; break; }
        }
        if (playIdx < 0) {
            for (int i = 0; i < h.size(); i++) {
                if (h.get(i).canPlayOn(m.topCard, m.currentColor, m.pendingDraws, m.mode == 1)) { playIdx = i; break; }
            }
        }
        if (playIdx >= 0) {
            UnoCardS card = h.get(playIdx);
            int colorChar = card.isWild() ? m.pickColor(h) : -1;
            unoPlay(room, user, playIdx, colorChar);
            return;
        }
        // 2) 无可出：被加牌则直接认罚，否则摸一张
        if (m.pendingDraws > 0) {
            m.drawCards(user, m.pendingDraws);
            m.pendingDraws = 0;
            m.advance(false);
            pushUnoState(room);
            scheduleUnoBotIfNeeded(room);
            return;
        }
        m.drawCards(user, 1);
        if (!h.isEmpty()) {
            int last = h.size() - 1;
            if (h.get(last).canPlayOn(m.topCard, m.currentColor, 0, m.mode == 1)) {
                UnoCardS card = h.get(last);
                int colorChar = card.isWild() ? m.pickColor(h) : -1;
                unoPlay(room, user, last, colorChar);
                return;
            }
        }
        m.advance(false);
        pushUnoState(room);
        scheduleUnoBotIfNeeded(room);
    }

    /** 游戏总时间到点：按当前手牌结算（全员 FAIL，按手牌扣分排名） */
    private static void scheduleGameEnd(DuelRoom room) {
        UnoMatch m = room.uno;
        if (m == null) return;
        if (m.gameTimerTask != null) m.gameTimerTask.cancel(false);
        m.gameTimerTask = matchScheduler.schedule(() -> {
            synchronized (room) {
                if (room.uno != m || m.gameOver) return;
                unoEnd(room, null);
            }
        }, m.matchDurationMs, TimeUnit.MILLISECONDS);
    }

    private static void pushUnoState(DuelRoom room) {
        UnoMatch m = room.uno;
        if (m == null) return;
        for (String p : room.players.keySet()) {
            pushToUser(p, "系统", "UNO_STATE:" + m.serialize(p));
        }
    }

    private static void unoEnd(DuelRoom room, String winner) {
        UnoMatch m = room.uno;
        if (m == null) return;
        m.gameOver = true;
        if (m.gameTimerTask != null) { m.gameTimerTask.cancel(false); m.gameTimerTask = null; }
        if (m.turnTimerTask != null) { m.turnTimerTask.cancel(false); m.turnTimerTask = null; }
        m.winner = winner == null ? "" : winner;
        // 重置准备状态，避免下一局房主/房客的 ready 残留导致需"取消再准备"
        room.gameStarted = false;
        for (String p : room.players.keySet()) room.players.put(p, false);
        // 先把"刚打完全手"的最后一手状态推出去（含 gameOver=1，但客户端只用来标记动画）
        // 服务端的赢家/分数在 DUEL_GAME_OVER 里再通告。
        pushUnoState(room);
        // 准备结算载荷
        final StringBuilder sb = new StringBuilder();
        for (String p : m.order) {
            int score = m.handScore(p);
            if (winner != null && p.equals(winner)) sb.append(p).append(",WIN:0:0");
            else sb.append(p).append(",FAIL:0:").append(score);
            sb.append(";");
        }
        final String overData = sb.toString();
        final int roomId = room.roomId;
        final DuelRoom refRoom = room;
        // 延迟推结算，让客户端把最后一张牌飞出动画播完（≈ 0.36s）
        matchScheduler.schedule(() -> {
            synchronized (refRoom) {
                if (refRoom.uno == null) return; // 中途已离开
                String overMsg = "DUEL_GAME_OVER:" + roomId + ":" + overData;
                for (String p : refRoom.players.keySet()) pushToUser(p, "系统", overMsg);
                if (m.botTask != null) m.botTask.cancel(false);
                if (m.drawDecisionTask != null) m.drawDecisionTask.cancel(false);
                List<String> bots = new ArrayList<>();
                for (String p : refRoom.players.keySet()) if (p.startsWith("机器人")) bots.add(p);
                for (String b : bots) refRoom.players.remove(b);
                for (String p : refRoom.players.keySet()) refRoom.players.put(p, false);
                refRoom.gameStarted = false;
                refRoom.uno = null;
                String ns = refRoom.getState();
                for (String p : refRoom.players.keySet()) pushToUser(p, "系统", "DUEL_STATE:" + ns);
            }
        }, 1100, TimeUnit.MILLISECONDS);
    }

    private static final Map<Integer, DuelRoom> duelRooms = new ConcurrentHashMap<>();
    private static final AtomicInteger roomIdGen = new AtomicInteger(1000);

    // ===== 匹配系统 =====
    private static final ScheduledExecutorService matchScheduler = Executors.newScheduledThreadPool(4);
    private static final long TURN_LIMIT_MS = 15000; // 出牌 / 质疑 / 变色 统一限时 15 秒
    private static int botCounter = 0;

    static class MatchEntry {
        int roomId;
        String mode;
        int maxPlayers;
        long startTime;
        java.util.concurrent.ScheduledFuture<?> botTask; // 60秒后补机器人的定时任务
        MatchEntry(int roomId, String mode, int maxPlayers) {
            this.roomId = roomId;
            this.mode = mode;
            this.maxPlayers = maxPlayers;
            this.startTime = System.currentTimeMillis();
        }
    }
    // key: "mode:maxPlayers" → 匹配中的房间列表
    private static final Map<String, List<MatchEntry>> matchQueues = new ConcurrentHashMap<>();

    /** 尝试合并两个匹配中的房间（把 fromRoom 的玩家全部移到 toRoom） */
    private static synchronized void mergeMatchedRooms(DuelRoom toRoom, DuelRoom fromRoom) {
        // 把 fromRoom 的所有玩家移入 toRoom
        List<String> movedPlayers = new ArrayList<>();
        for (String p : fromRoom.players.keySet()) {
            if (toRoom.players.size() >= toRoom.maxPlayers) break;
            if (!toRoom.players.containsKey(p)) {
                toRoom.players.put(p, false);
                movedPlayers.add(p);
            }
        }
        // 删除旧房间
        int oldRoomId = fromRoom.roomId;
        duelRooms.remove(oldRoomId);
        // 从匹配队列移除旧房间
        String key = toRoom.mode + ":" + toRoom.maxPlayers;
        List<MatchEntry> queue = matchQueues.get(key);
        if (queue != null) queue.removeIf(e -> e.roomId == oldRoomId);

        // 推送新状态给目标房间所有玩家
        String newState = toRoom.getState();
        System.out.println("[匹配] 合并后房间状态: " + newState);
        for (String p : toRoom.players.keySet()) {
            pushToUser(p, "系统", "DUEL_STATE:" + newState);
        }
        // 通知被移动的玩家切换到新房间
        for (String p : movedPlayers) {
            pushToUser(p, "系统", "DUEL_MATCH_MOVE:" + toRoom.roomId + ":" + toRoom.mode + ":" + toRoom.maxPlayers);
        }
        System.out.println("[匹配] 合并房间: " + oldRoomId + " → " + toRoom.roomId + ", 移动玩家: " + movedPlayers);

        // 如果目标房间满了，从匹配队列移除
        if (toRoom.players.size() >= toRoom.maxPlayers) {
            if (queue != null) queue.removeIf(e -> e.roomId == toRoom.roomId);
        }
    }

    /** 给房间补充机器人 */
    private static void addBotsToRoom(int roomId) {
        DuelRoom room = duelRooms.get(roomId);
        if (room == null || room.gameStarted) return;
        synchronized (room) {
            int need = room.maxPlayers - room.players.size();
            if (need <= 0) return;
            for (int i = 0; i < need; i++) {
                String botName = "机器人" + (++botCounter);
                room.players.put(botName, true); // 机器人自动准备
                System.out.println("[匹配] 房间 " + roomId + " 加入机器人: " + botName);
            }
            // 推送新状态 + 机器人加入通知给人类玩家
            String newState = room.getState();
            for (String p : room.players.keySet()) {
                if (!p.startsWith("机器人")) {
                    pushToUser(p, "系统", "DUEL_STATE:" + newState);
                    pushToUser(p, "系统", "DUEL_BOTS_JOINED:" + roomId);
                }
            }
        }
        // 从匹配队列移除
        removeFromMatchQueue(roomId);
    }

    /** 从匹配队列中移除指定房间，并取消定时机器人任务 */
    private static void removeFromMatchQueue(int roomId) {
        for (List<MatchEntry> queue : matchQueues.values()) {
            queue.removeIf(e -> {
                if (e.roomId == roomId) {
                    if (e.botTask != null) e.botTask.cancel(false);
                    return true;
                }
                return false;
            });
        }
    }

    static class PushSession {
        PrintWriter out;
        Socket socket;
        String username;
        PushSession(PrintWriter out, Socket socket, String username) {
            this.out = out;
            this.socket = socket;
            this.username = username;
        }
    }

    public static void main(String[] args) {
        System.out.println("=== FishGrabbingServer ===");
        System.out.println("正在初始化数据库连接...");
        dbManager = new DatabaseManager();
        if (!dbManager.testConnection()) {
            System.out.println("数据库连接失败");
            return;
        }
        System.out.println("数据库连接成功！");
        startServer();
    }

    private static void pushToUser(String receiverName, String senderName, String message) {
        PushSession session = pushClients.get(receiverName);
        if (session != null) {
            try {
                session.out.println("PUSH|" + senderName + "|" + message);
                System.out.println("[推送] -> " + receiverName + " | " + senderName + " | " + message);
            } catch (Exception e) {
                System.out.println("[推送] 发送失败，移除 " + receiverName + ": " + e.getMessage());
                pushClients.remove(receiverName);
            }
        } else {
            System.out.println("[推送] 无会话，无法推送给 " + receiverName + " | " + message);
        }
    }

    /** 好友申请中转：A申请加B → 存储到数据库 + 推送通知给B */
    private static String handleFriendRequest(String requester, String target) {
        // 检查目标用户是否存在
        int targetId = -1;
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                server.db.DatabaseConfig.getUrl(),
                server.db.DatabaseConfig.USERNAME,
                server.db.DatabaseConfig.PASSWORD)) {
            targetId = getTargetId(conn, target);
        } catch (Exception e) {
            return "ERROR|查询用户失败";
        }
        if (targetId == -1) return "ERROR|该用户不存在";

        int requesterId = -1;
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                server.db.DatabaseConfig.getUrl(),
                server.db.DatabaseConfig.USERNAME,
                server.db.DatabaseConfig.PASSWORD)) {
            requesterId = getUserId(conn, requester);
            if (requesterId == -1) return "ERROR|发送者不存在";
            if (requesterId ==+ targetId) return "ERROR|不能添加自己为好友";
            try (java.sql.PreparedStatement pstmt = conn.prepareStatement(
                    "SELECT 1 FROM friends WHERE (User_id = ? AND Friend_id = ?) OR (User_id = ? AND Friend_id = ?)")) {
                pstmt.setInt(1, requesterId);
                pstmt.setInt(2, targetId);
                pstmt.setInt(3, targetId);
                pstmt.setInt(4, requesterId);
                try (java.sql.ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) return "ERROR|已经是好友了";
                }
            }
        } catch (Exception e) {
            return "ERROR|检查好友关系失败";
        }

        // 存储好友申请消息到数据库 + 推送通知
        String notifyMsg = "FRIEND_REQUEST:" + requester;
        String storeResult = dbManager.sendMessage("moyu官方", target, notifyMsg, 0);
        if (!storeResult.startsWith("SUCCESS")) return storeResult;
        pushToUser(target, "moyu官方", notifyMsg);
        System.out.println("[好友申请] " + requester + " -> " + target + "，已存储+推送");
        return "SUCCESS";
    }

    /** 同意好友：B 同意 A → 添加好友 + 存记录 + 推送通知给A */
    private static String handleFriendAccept(String message, String responder, String requester) {
        String[] parts = message.split(",", 2);
        if (parts.length < 2) return "ERROR|无效格式";
        String requesterName = parts[0].substring("FRIEND_ACCEPT:".length());

        String addResult = dbManager.addFriend(requesterName, responder);
        if (!addResult.startsWith("SUCCESS")) return addResult;

        // 申请者收到：对方已成为你的好友
        String notifyApplicant = responder + "已成为你的好友！";
        dbManager.sendMessage("moyu官方", requesterName, notifyApplicant, 0);
        pushToUser(requesterName, "moyu官方", notifyApplicant);

        // 同意方收到：你已成为对方的好友
        String notifyResponder = "你已成为" + requesterName + "的好友！";
        dbManager.sendMessage("moyu官方", responder, notifyResponder, 0);
        pushToUser(responder, "moyu官方", notifyResponder);
        System.out.println("[好友同意] " + responder + " 同意 " + requesterName + " 的好友申请");
        return "SUCCESS";
    }

    /** 拒绝好友：B 拒绝 A → 存记录 + 推送通知给A */
    private static String handleFriendReject(String message, String responder, String requester) {
        String[] parts = message.split(",", 2);
        if (parts.length < 2) return "ERROR|无效格式";
        String requesterName = parts[0].substring("FRIEND_REJECT:".length());

        String notifyMsg = requesterName + "拒绝成为你的好友！";
        dbManager.sendMessage("moyu官方", requesterName, notifyMsg, 0);
        pushToUser(requesterName, "moyu官方", notifyMsg);
        System.out.println("[好友拒绝] " + responder + " 拒绝 " + requesterName + " 的好友申请");
        return "SUCCESS";
    }

    private static int getTargetId(java.sql.Connection conn, String nameOrAccount) throws java.sql.SQLException {
        try (java.sql.PreparedStatement pstmt = conn.prepareStatement(
                "SELECT User_id FROM users WHERE User_name = ? OR User_account = ? LIMIT 1")) {
            pstmt.setString(1, nameOrAccount);
            pstmt.setString(2, nameOrAccount);
            try (java.sql.ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) return rs.getInt("User_id");
            }
        }
        return -1;
    }

    private static int getUserId(java.sql.Connection conn, String nameOrAccount) throws java.sql.SQLException {
        try (java.sql.PreparedStatement pstmt = conn.prepareStatement(
                "SELECT User_id FROM users WHERE User_name = ? OR User_account = ? LIMIT 1")) {
            pstmt.setString(1, nameOrAccount);
            pstmt.setString(2, nameOrAccount);
            try (java.sql.ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) return rs.getInt("User_id");
            }
        }
        return -1;
    }

    private static void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("服务器已启动，端口: " + PORT);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(() -> handleConnection(clientSocket));
            }
        } catch (Exception e) {
            System.err.println("服务器启动失败: " + e.getMessage());
        }
    }

    private static void handleConnection(Socket socket) {
        int connCount = activeConnections.incrementAndGet();
        boolean acquired = false;
        try {
            if (!connLimit.tryAcquire()) {
                PrintWriter busyOut = new PrintWriter(socket.getOutputStream(), true);
                busyOut.println("ERROR|服务器繁忙，请稍后重试");
                return;
            }
            acquired = true;
            socket.setSoTimeout(30000);
            System.out.println("[连接] #" + connCount + " 从 " + socket.getInetAddress());

            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            String firstLine = in.readLine();
            if (firstLine == null) return;

            // 推送长连接
            if (firstLine.startsWith("PUSH_REGISTER|")) {
                String username = firstLine.substring("PUSH_REGISTER|".length());
                PushSession old = pushClients.put(username, new PushSession(out, socket, username));
                if (old != null && old.socket != socket) {
                    try { old.socket.close(); } catch (Exception ignored) {}
                }
                out.println("PUSH_OK");
                System.out.println("推送注册: " + username);

                // 保持长连接，监听心跳
                String line;
                while ((line = in.readLine()) != null) {
                    if ("PING".equals(line.trim())) out.println("PONG");
                }
                pushClients.remove(username);
                System.out.println("推送断开: " + username);
                return;
            }

            // 推送注销
            if (firstLine.startsWith("PUSH_UNREGISTER|")) {
                String username = firstLine.substring("PUSH_UNREGISTER|".length());
                pushClients.remove(username);
                out.println("PUSH_DISCONNECTED");
                return;
            }

            // 普通请求循环
            String line = firstLine;
            while (line != null) {
                String[] parts = line.split("\\|", -1);
                if (parts.length < 2) { out.println("ERROR|无效请求格式"); }
                else {
                    String action = parts[0].toUpperCase();

                    if ("SENDMESSAGE".equals(action) && parts.length >= 4) {
                        String sender = parts[1];
                        String receiver = parts[2];
                        String message = parts[3];
                        String response;

                        // 好友申请：A → B，通过 moyu官方 推送通知给B
                        if (message.startsWith("FRIEND_REQUEST:")) {
                            response = handleFriendRequest(sender, receiver);
                        }
                        // 同意好友：B 同意 A，直接加双向好友关系，通知A
                        else if (message.startsWith("FRIEND_ACCEPT:")) {
                            response = handleFriendAccept(message, sender, receiver);
                        }
                        // 拒绝好友：B 拒绝 A，通知A
                        else if (message.startsWith("FRIEND_REJECT:")) {
                            response = handleFriendReject(message, sender, receiver);
                        }
                        else {
                            response = dbManager.sendMessage(sender, receiver, message, 0);
                            if (response.startsWith("SUCCESS")) {
                                pushToUser(receiver, sender, message);
                            }
                        }
                        out.println(response);
                    } else {
                        out.println(processRequest(line));
                    }
                }
                try {
                    line = in.readLine();
                } catch (SocketTimeoutException e) {
                    // 客户端无更多请求，正常超时退出
                    break;
                } catch (Exception e) {
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("连接异常: " + e.getMessage());
        } finally {
            if (acquired) connLimit.release();
            activeConnections.decrementAndGet();
            try { socket.close(); } catch (Exception ignored) {}
        }
    }

    private static String processRequest(String request) {
        String[] parts = request.split("\\|", -1);
        if (parts.length < 2) return "ERROR|无效请求格式";
        String action = parts[0].toUpperCase();
        switch (action) {
            case "REGISTER":
                if (parts.length != 4) return "ERROR|注册需要3个参数";
                return dbManager.register(parts[1], parts[2], parts[3]);
            case "LOGIN":
                if (parts.length != 3) return "ERROR|登录需要2个参数";
                return dbManager.login(parts[1], parts[2]);
            case "CHANGEPASSWORD":
                if (parts.length != 4) return "ERROR|修改密码需要3个参数";
                return dbManager.changePassword(parts[1], parts[2], parts[3]);
            case "GETRECORDS":
                if (parts.length == 2) {
                    try { return dbManager.getRecords(Integer.parseInt(parts[1])); }
                    catch (NumberFormatException e) { return "ERROR|用户ID格式错误"; }
                } else if (parts.length == 3) {
                    try { return dbManager.getRecords(Integer.parseInt(parts[1]), parts[2]); }
                    catch (NumberFormatException e) { return "ERROR|用户ID格式错误"; }
                } else {
                    return "ERROR|读取纪录需要1~2个参数";
                }
            case "SAVERECORD":
                if (parts.length != 5) return "ERROR|保存纪录需要4个参数";
                try { return dbManager.saveRecord(Integer.parseInt(parts[1]), parts[2], parts[3], parts[4]); }
                catch (NumberFormatException e) { return "ERROR|用户ID格式错误"; }
            case "GETUSERSTATE":
                if (parts.length != 2) return "ERROR|查询状态需要1个参数";
                return dbManager.getUserState(parts[1]);
            case "SETUSERSTATE":
                if (parts.length != 3) return "ERROR|设置状态需要2个参数";
                try { return dbManager.setUserState(parts[1], Integer.parseInt(parts[2])); }
                catch (NumberFormatException e) { return "ERROR|状态值格式错误"; }
            case "ADDFRIEND":
                if (parts.length != 3) return "ERROR|添加好友需要2个参数";
                return dbManager.addFriend(parts[1], parts[2]);
            case "DELETEFRIEND":
                if (parts.length != 3) return "ERROR|删除好友需要2个参数";
                return dbManager.deleteFriend(parts[1], parts[2]);
            case "GETFRIENDID":
                if (parts.length != 2) return "ERROR|查询好友ID需要1个参数";
                return dbManager.getUserId(parts[1]);
            case "GETFRIENDS":
                if (parts.length != 2) return "ERROR|查询好友需要1个参数";
                return dbManager.getFriends(parts[1]);
            case "GETMESSAGES":
                if (parts.length != 2) return "ERROR|查询消息需要1个参数";
                return dbManager.getMessages(parts[1]);
            case "GETUNREADCOUNT":
                if (parts.length != 2) return "ERROR|查询未读需要1个参数";
                return dbManager.getUnreadCount(parts[1]);
            case "MARKALLREAD":
                if (parts.length != 2) return "ERROR|标记已读需要1个参数";
                return dbManager.markAllRead(parts[1]);
            case "GETRECENTCHAT":
                if (parts.length != 4) return "ERROR|最近聊天需要3个参数";
                try { return dbManager.getRecentChat(parts[1], parts[2], Integer.parseInt(parts[3])); }
                catch (NumberFormatException e) { return "ERROR|条数格式错误"; }
            case "GETRECENTCHATUNREAD":
                if (parts.length != 4) return "ERROR|聊天(未读)需要3个参数";
                try { return dbManager.getRecentChatWithUnread(parts[1], parts[2], Integer.parseInt(parts[3])); }
                catch (NumberFormatException e) { return "ERROR|条数格式错误"; }
            case "DUEL_MATCH":
                // DUEL_MATCH|roomId|username|mode|maxPlayers
                if (parts.length != 5) return "ERROR|匹配请求需要4个参数";
                try {
                    int mId = Integer.parseInt(parts[1]);
                    String mUser = parts[2];
                    String mMode = parts[3];
                    int mMax = Integer.parseInt(parts[4]);
                    DuelRoom mRoom = duelRooms.get(mId);
                    if (mRoom == null) return "ERROR|房间不存在";
                    if (!mRoom.creator.equals(mUser)) return "ERROR|只有房主可以发起匹配";

                    String key = mMode + ":" + mMax;
                    List<MatchEntry> queue = matchQueues.computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>()));

                    // 查找可以合并的房间
                    boolean merged = false;
                    synchronized (matchQueues) {
                        for (MatchEntry entry : new ArrayList<>(queue)) {
                            DuelRoom otherRoom = duelRooms.get(entry.roomId);
                            if (otherRoom == null || otherRoom.roomId == mId) continue;
                            if (otherRoom.players.size() + mRoom.players.size() <= mMax) {
                                // 合并：把当前房间的玩家移到目标房间
                                mergeMatchedRooms(otherRoom, mRoom);
                                merged = true;
                                break;
                            }
                        }
                    }
                    if (!merged) {
                        // 没找到匹配，加入队列，启动60s倒计时
                        MatchEntry entry = new MatchEntry(mId, mMode, mMax);
                        // 推送匹配开始通知
                        pushToUser(mUser, "系统", "DUEL_MATCH_START:" + mId + ":60");
                        // 60秒后自动加入机器人
                        entry.botTask = matchScheduler.schedule(() -> addBotsToRoom(mId), 60, TimeUnit.SECONDS);
                        queue.add(entry);
                        System.out.println("[匹配] 房间 " + mId + " 开始匹配 (" + mMode + "/" + mMax + "人)");
                    }
                    return "SUCCESS|MATCHING";
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "DUEL_MATCH_CANCEL":
                // DUEL_MATCH_CANCEL|roomId|username
                if (parts.length != 3) return "ERROR|取消匹配需要2个参数";
                try {
                    int cmId = Integer.parseInt(parts[1]);
                    removeFromMatchQueue(cmId);
                    System.out.println("[匹配] 房间 " + cmId + " 取消匹配");
                    return "SUCCESS|CANCELLED";
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "DUEL_ADD_BOT":
                // DUEL_ADD_BOT|roomId|username → 房主手动给房间添加一个机器人
                if (parts.length != 3) return "ERROR|添加机器人需要2个参数";
                try {
                    int abId = Integer.parseInt(parts[1]);
                    String abUser = parts[2];
                    DuelRoom abRoom = duelRooms.get(abId);
                    if (abRoom == null) return "ERROR|房间不存在";
                    if (!abRoom.creator.equals(abUser)) return "ERROR|只有房主可以添加机器人";
                    synchronized (abRoom) {
                        if (abRoom.players.size() >= abRoom.maxPlayers) return "ERROR|房间已满";
                        String botName = "机器人" + (++botCounter);
                        abRoom.players.put(botName, true); // 机器人自动准备
                        String newState = abRoom.getState();
                        for (String p : abRoom.players.keySet()) {
                            if (!p.startsWith("机器人")) {
                                pushToUser(p, "系统", "DUEL_STATE:" + newState);
                                pushToUser(p, "系统", "DUEL_BOTS_JOINED:" + abId);
                            }
                        }
                        System.out.println("[自定义] 房间 " + abId + " 手动加入机器人: " + botName);
                    }
                    return "SUCCESS";
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "DUEL_CREATE":
                if (parts.length != 5) return "ERROR|创建对决需要4个参数";
                try {
                    int roomId = roomIdGen.incrementAndGet();
                    String creator = parts[1];
                    String mode = parts[2];
                    int maxPlayers = Integer.parseInt(parts[3]);
                    String gameType = parts[4];
                    DuelRoom room = new DuelRoom(roomId, creator, mode, maxPlayers, gameType);
                    duelRooms.put(roomId, room);
                    return "SUCCESS|" + room.getState();
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "DUEL_JOIN":
                if (parts.length != 3) return "ERROR|加入对决需要2个参数";
                try {
                    int rId = Integer.parseInt(parts[1]);
                    String joiner = parts[2];
                    DuelRoom room = duelRooms.get(rId);
                    if (room == null) return "ERROR|房间不存在或已解散";
                    String result = room.joinRoom(joiner);
                    if (result.startsWith("SUCCESS")) {
                        // 推送完整房间状态给所有人
                        String newState = room.getState();
                        for (String p : room.players.keySet()) {
                            pushToUser(p, "系统", "DUEL_STATE:" + newState);
                        }
                    }
                    return result;
                } catch (NumberFormatException e) { return "ERROR|房间ID格式错误"; }
            case "DUEL_LEAVE":
                if (parts.length != 3) return "ERROR|离开对决需要2个参数";
                try {
                    int rid = Integer.parseInt(parts[1]);
                    String leaver = parts[2];
                    DuelRoom room = duelRooms.get(rid);
                    if (room == null) return "ERROR|房间不存在";
                    String res = room.leaveRoom(leaver);
                    // 对局进行中有人离开 → 结束本局（胜者留空，全员按手牌计分）
                    synchronized (room) { if (room.uno != null) unoEnd(room, null); }
                    if (room.players.isEmpty()) {
                        duelRooms.remove(rid);
                        removeFromMatchQueue(rid);
                    } else {
                        // 推送完整房间状态给剩余玩家（room.getState() 已包含 roomId）
                        String newState = room.getState();
                        for (String p : room.players.keySet()) {
                            pushToUser(p, "系统", "DUEL_STATE:" + newState);
                        }
                    }
                    return res;
                } catch (NumberFormatException e) { return "ERROR|房间ID格式错误"; }
            case "DUEL_READY":
                if (parts.length != 3) return "ERROR|准备需要2个参数";
                try {
                    int ri = Integer.parseInt(parts[1]);
                    String user = parts[2];
                    DuelRoom room = duelRooms.get(ri);
                    if (room == null) return "ERROR|房间不存在";
                    String res = room.toggleReady(user);
                    System.out.println("[对决] DUEL_READY 结果: " + res);
                    // 通知所有人状态变更
                    if (res.startsWith("SUCCESS")) {
                        // 推送完整房间状态给所有玩家（room.getState() 已包含 roomId）
                        String fullState = room.getState();
                        System.out.println("[对决] 准备后推送状态: " + fullState);
                        for (String p : room.players.keySet()) {
                            pushToUser(p, "系统", "DUEL_STATE:" + fullState);
                        }
                        // 如果全部准备，生成种子并推送游戏开始
                        if (res.contains("ALL_READY")) {
                            room.gameStarted = true;
                            room.gameSeed = System.currentTimeMillis() ^ (ri * 7919L);
                            room.gameResults.clear(); // 清空上局残留
                            room.stop2048Bots(); // 清理上局机器人状态
                            String startMsg = "DUEL_GAME_START:" + ri + ":" + room.gameSeed + ":" + room.mode;
                            for (String p : room.players.keySet()) {
                                pushToUser(p, "系统", startMsg);
                            }
                            System.out.println("[对决] 房间 " + ri + " 游戏开始，种子=" + room.gameSeed);
                            // 2048 房间启动机器人 AI；UNO 由服务端建权威对局
                            if ("2048".equals(room.gameType)) {
                                room.start2048Bots(matchScheduler);
                            } else if ("UNO".equals(room.gameType)) {
                                int unoMode = 0;
                                try { unoMode = Integer.parseInt(room.mode); } catch (Exception ignored) {}
                                room.uno = new UnoMatch(room, unoMode, room.gameSeed);
                                // DUEL_GAME_START 上面已统一推送过一次，此处不要重复推，
                                // 否则客户端会收到两条开局消息（靠 gameStarting 兜底也没必要）
                                pushUnoState(room);
                                scheduleUnoBotIfNeeded(room);
                                scheduleGameEnd(room);
                                scheduleTurnTimer(room);
                                System.out.println("[UNO] 房间 " + ri + " 开局，玩家=" + room.uno.order);
                            }
                        }
                    }
                    return res;
                } catch (NumberFormatException e) { return "ERROR|房间ID格式错误"; }
            case "DUEL_INFO":
                if (parts.length != 2) return "ERROR|查询房间需要1个参数";
                try {
                    int r = Integer.parseInt(parts[1]);
                    DuelRoom room = duelRooms.get(r);
                    if (room == null) return "ERROR|房间不存在";
                    return "SUCCESS|" + room.getState();
                } catch (NumberFormatException e) { return "ERROR|房间ID格式错误"; }
            case "DUEL_UPDATE_MODE":
                // DUEL_UPDATE_MODE|roomId|username|mode → 房主开局前修改玩法模式（UNO 用 0/1）
                if (parts.length != 4) return "ERROR|更新模式需要3个参数";
                try {
                    int umId = Integer.parseInt(parts[1]);
                    String umUser = parts[2];
                    String umMode = parts[3];
                    DuelRoom umRoom = duelRooms.get(umId);
                    if (umRoom == null) return "ERROR|房间不存在";
                    synchronized (umRoom) {
                        if (!umRoom.creator.equals(umUser)) return "ERROR|只有房主可以修改玩法";
                        if (umRoom.gameStarted) return "ERROR|游戏已开始";
                        umRoom.mode = umMode;
                        String umState = umRoom.getState();
                        for (String p : umRoom.players.keySet()) {
                            pushToUser(p, "系统", "DUEL_STATE:" + umState);
                        }
                        return "SUCCESS|" + umState;
                    }
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "DUEL_UPDATE_MAX":
                if (parts.length != 4) return "ERROR|更新人数需要3个参数";
                try {
                    int rid = Integer.parseInt(parts[1]);
                    String requester = parts[2];
                    int newMax = Integer.parseInt(parts[3]);
                    DuelRoom room = duelRooms.get(rid);
                    if (room == null) return "ERROR|房间不存在";
                    String res = room.setMaxPlayers(requester, newMax);
                    // 推送新状态给房间内所有其他玩家
                    if (res.startsWith("SUCCESS")) {
                        String newState = room.getState();
                        for (String p : room.players.keySet()) {
                            if (!p.equals(requester)) {
                                pushToUser(p, "系统", "DUEL_STATE:" + newState);
                            }
                        }
                    }
                    return res;
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            // ===== 对决游戏内操作 =====
            case "DUEL_GAME_REVEAL":
                if (parts.length != 6) return "ERROR|翻开操作格式错误";
                try {
                    int grId = Integer.parseInt(parts[1]);
                    String gUser = parts[2];
                    int gRow = Integer.parseInt(parts[3]);
                    int gCol = Integer.parseInt(parts[4]);
                    int gVal = Integer.parseInt(parts[5]);
                    DuelRoom gRoom = duelRooms.get(grId);
                    if (gRoom == null) return "ERROR|房间不存在";
                    // 广播给其他玩家（包含 roomId）
                    String revealMsg = "DUEL_GAME_PUSH:" + grId + ":" + gUser + ":REVEAL:" + gRow + ":" + gCol + ":" + gVal;
                    for (String p : gRoom.players.keySet()) {
                        if (!p.equals(gUser)) {
                            pushToUser(p, "系统", revealMsg);
                        }
                    }
                    return "SUCCESS";
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "DUEL_GAME_FLAG":
                if (parts.length != 5) return "ERROR|插旗操作格式错误";
                try {
                    int gfId = Integer.parseInt(parts[1]);
                    String gfUser = parts[2];
                    int gfRow = Integer.parseInt(parts[3]);
                    int gfCol = Integer.parseInt(parts[4]);
                    DuelRoom gfRoom = duelRooms.get(gfId);
                    if (gfRoom == null) return "ERROR|房间不存在";
                    String flagMsg = "DUEL_GAME_PUSH:" + gfId + ":" + gfUser + ":FLAG:" + gfRow + ":" + gfCol;
                    for (String p : gfRoom.players.keySet()) {
                        if (!p.equals(gfUser)) {
                            pushToUser(p, "系统", flagMsg);
                        }
                    }
                    return "SUCCESS";
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "DUEL_GAME_UNFLAG":
                if (parts.length != 5) return "ERROR|取消插旗格式错误";
                try {
                    int guId = Integer.parseInt(parts[1]);
                    String guUser = parts[2];
                    int guRow = Integer.parseInt(parts[3]);
                    int guCol = Integer.parseInt(parts[4]);
                    DuelRoom guRoom = duelRooms.get(guId);
                    if (guRoom == null) return "ERROR|房间不存在";
                    String unflagMsg = "DUEL_GAME_PUSH:" + guId + ":" + guUser + ":UNFLAG:" + guRow + ":" + guCol;
                    for (String p : guRoom.players.keySet()) {
                        if (!p.equals(guUser)) {
                            pushToUser(p, "系统", unflagMsg);
                        }
                    }
                    return "SUCCESS";
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "DUEL_GAME_RESULT":
                if (parts.length < 5) return "ERROR|结果格式错误";
                try {
                    int drId = Integer.parseInt(parts[1]);
                    String drUser = parts[2];
                    String drResult = parts[3];
                    long drTime = Long.parseLong(parts[4]);
                    int drScore = parts.length >= 6 ? Integer.parseInt(parts[5]) : 0;
                    DuelRoom drRoom = duelRooms.get(drId);
                    if (drRoom == null) return "ERROR|房间不存在";
                    String res = drRoom.recordGameResult(drUser, drResult, drTime, drScore);
                    if (res.startsWith("SUCCESS|ALL_DONE")) {
                        // 所有人出结果，推送游戏结束
                        String overData = res.substring("SUCCESS|ALL_DONE|".length());
                        String overMsg = "DUEL_GAME_OVER:" + drId + ":" + overData;
                        for (String p : drRoom.players.keySet()) {
                            pushToUser(p, "系统", overMsg);
                        }
                        // 移除所有机器人，释放空缺位
                        List<String> botsToRemove = new ArrayList<>();
                        for (String p : drRoom.players.keySet()) {
                            if (p.startsWith("机器人")) {
                                botsToRemove.add(p);
                            }
                        }
                        for (String bot : botsToRemove) {
                            drRoom.players.remove(bot);
                            System.out.println("[对决] 房间 " + drId + " 对局结束，机器人 " + bot + " 退出");
                        }
                        // 重置剩余玩家 ready 状态为未准备
                        for (String p : drRoom.players.keySet()) {
                            drRoom.players.put(p, false);
                        }
                        drRoom.gameStarted = false;
                        // 游戏结果保留，供 DUEL_GAME_RESULTS 轮询使用；
                        // 新游戏开始（DUEL_READY）时会自动清空上局残留。
                        // 推送新状态给房间所有人
                        String newState = drRoom.getState();
                        for (String p : drRoom.players.keySet()) {
                            pushToUser(p, "系统", "DUEL_STATE:" + newState);
                        }
                        System.out.println("[对决] 房间 " + drId + " 游戏结束：" + overData + "，剩余 " + drRoom.players.size() + " 人");
                    }
                    return "SUCCESS";
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "DUEL_GAME_RESULTS":
                // 轮询查询当前房间的游戏结果（兜底机制）
                if (parts.length != 2) return "ERROR|查询结果需要1个参数";
                try {
                    int crId = Integer.parseInt(parts[1]);
                    DuelRoom crRoom = duelRooms.get(crId);
                    if (crRoom == null) return "ERROR|房间不存在";
                    synchronized (crRoom) {
                        if (crRoom.gameResults.size() >= crRoom.players.size()) {
                            return "SUCCESS|ALL_DONE|" + crRoom.getGameOverData();
                        }
                        // 返回当前已有结果
                        StringBuilder sb = new StringBuilder("SUCCESS|WAITING");
                        for (Map.Entry<String, String> e : crRoom.gameResults.entrySet()) {
                            sb.append("|").append(e.getKey()).append(",").append(e.getValue());
                        }
                        return sb.toString();
                    }
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "DUEL_GAME_STATE":
                // DUEL_GAME_STATE|roomId → 获取游戏启动状态（兜底机制）
                if (parts.length != 2) return "ERROR|查询游戏状态需要1个参数";
                try {
                    int gsId = Integer.parseInt(parts[1]);
                    DuelRoom gsRoom = duelRooms.get(gsId);
                    if (gsRoom == null) return "ERROR|房间不存在";
                    if (gsRoom.gameStarted) {
                        return "SUCCESS|STARTED|" + gsRoom.gameSeed + "|" + gsRoom.mode;
                    }
                    return "SUCCESS|NOT_STARTED";
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "DUEL_GAME_SCORE":
                // DUEL_GAME_SCORE|roomId|username|score → 广播分数给房间内其他玩家（兼容旧客户端）
                if (parts.length != 4) return "ERROR|分数同步需要3个参数";
                try {
                    int scId = Integer.parseInt(parts[1]);
                    String scUser = parts[2];
                    int scVal = Integer.parseInt(parts[3]);
                    DuelRoom scRoom = duelRooms.get(scId);
                    if (scRoom == null) return "ERROR|房间不存在";
                    String scorePush = "DUEL_SCORE_PUSH:" + scId + ":" + scUser + ":" + scVal;
                    for (String p : scRoom.players.keySet()) {
                        if (!p.equals(scUser)) {
                            pushToUser(p, "系统", scorePush);
                        }
                    }
                    return "SUCCESS";
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "DUEL_GAME_BOARD":
                // DUEL_GAME_BOARD|roomId|username|score|boardData → 广播完整局面给房间内其他玩家
                if (parts.length != 5) return "ERROR|局面同步需要4个参数";
                try {
                    int bdId = Integer.parseInt(parts[1]);
                    String bdUser = parts[2];
                    int bdScore = Integer.parseInt(parts[3]);
                    String bdData = parts[4];
                    DuelRoom bdRoom = duelRooms.get(bdId);
                    if (bdRoom == null) return "ERROR|房间不存在";
                    String boardPush = "DUEL_BOARD_PUSH:" + bdId + ":" + bdUser + ":" + bdScore + ":" + bdData;
                    for (String p : bdRoom.players.keySet()) {
                        if (!p.equals(bdUser)) {
                            pushToUser(p, "系统", boardPush);
                        }
                    }
                    return "SUCCESS";
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "DUEL_CHAT":
                // DUEL_CHAT|roomId|username|message（不存库，纯推送中转）
                if (parts.length != 4) return "ERROR|对决聊天需要3个参数";
                try {
                    int dcId = Integer.parseInt(parts[1]);
                    String dcUser = parts[2];
                    String dcMsg = parts[3];
                    DuelRoom dcRoom = duelRooms.get(dcId);
                    if (dcRoom == null) return "ERROR|房间不存在";
                    String chatPush = "DUEL_CHAT_PUSH:" + dcId + ":" + dcUser + ":" + dcMsg;
                    for (String p : dcRoom.players.keySet()) {
                        pushToUser(p, "系统", chatPush);
                    }
                    return "SUCCESS";
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            // ===== UNO 指令（服务端权威） =====
            case "UNO_PLAY":
                // UNO_PLAY|roomId|username|handIdx|colorChar(r/y/g/b 或 x)
                if (parts.length != 5) return "ERROR|出牌格式错误";
                try {
                    int ur = Integer.parseInt(parts[1]);
                    DuelRoom uRoom = duelRooms.get(ur);
                    if (uRoom == null) return "ERROR|房间不存在";
                    synchronized (uRoom) {
                        int hi = Integer.parseInt(parts[3]);
                        String cc = parts[4];
                        int colorChar = (cc != null && cc.length() == 1 && "RYGB".indexOf(cc) >= 0) ? "RYGB".indexOf(cc) : -1;
                        return unoPlay(uRoom, parts[2], hi, colorChar);
                    }
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "UNO_DRAW":
                if (parts.length != 3) return "ERROR|摸牌格式错误";
                try {
                    int ur = Integer.parseInt(parts[1]);
                    DuelRoom uRoom = duelRooms.get(ur);
                    if (uRoom == null) return "ERROR|房间不存在";
                    synchronized (uRoom) { return unoDraw(uRoom, parts[2]); }
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "UNO_DRAW_DECIDE":
                // UNO_DRAW_DECIDE|roomId|username|play(0/1)|colorChar(R/Y/G/B 或 x)
                if (parts.length != 5) return "ERROR|摸牌决策格式错误";
                try {
                    int ur = Integer.parseInt(parts[1]);
                    DuelRoom uRoom = duelRooms.get(ur);
                    if (uRoom == null) return "ERROR|房间不存在";
                    synchronized (uRoom) {
                        String cc = parts[4];
                        int colorChar = (cc != null && cc.length() == 1 && "RYGB".indexOf(cc) >= 0) ? "RYGB".indexOf(cc) : -1;
                        return unoDrawDecide(uRoom, parts[2], "1".equals(parts[3]), colorChar);
                    }
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "UNO_UNO":
                if (parts.length != 3) return "ERROR|喊 UNO 格式错误";
                try {
                    int ur = Integer.parseInt(parts[1]);
                    DuelRoom uRoom = duelRooms.get(ur);
                    if (uRoom == null) return "ERROR|房间不存在";
                    synchronized (uRoom) { return unoUno(uRoom, parts[2]); }
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "UNO_CATCH":
                // UNO_CATCH|roomId|catcher|target
                if (parts.length != 4) return "ERROR|抓 UNO 格式错误";
                try {
                    int ur = Integer.parseInt(parts[1]);
                    DuelRoom uRoom = duelRooms.get(ur);
                    if (uRoom == null) return "ERROR|房间不存在";
                    synchronized (uRoom) { return unoCatch(uRoom, parts[2], parts[3]); }
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "UNO_CHALLENGE":
                // UNO_CHALLENGE|roomId|username|accept(0/1)
                if (parts.length != 4) return "ERROR|质疑格式错误";
                try {
                    int ur = Integer.parseInt(parts[1]);
                    DuelRoom uRoom = duelRooms.get(ur);
                    if (uRoom == null) return "ERROR|房间不存在";
                    synchronized (uRoom) { return unoChallenge(uRoom, parts[2], "1".equals(parts[3])); }
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "UNO_STATE":
                // UNO_STATE|roomId|username → 返回该玩家的公开状态 + 私有手牌
                if (parts.length != 3) return "ERROR|查询状态格式错误";
                try {
                    int ur = Integer.parseInt(parts[1]);
                    DuelRoom uRoom = duelRooms.get(ur);
                    if (uRoom == null) return "ERROR|房间不存在";
                    synchronized (uRoom) {
                        if (uRoom.uno == null) return "ERROR|对局未开始";
                        return "SUCCESS|" + uRoom.uno.serialize(parts[2]);
                    }
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            case "UNO_END":
                // UNO_END|roomId|username → 提前结束（某人退出等），复用普通结束流程
                if (parts.length != 3) return "ERROR|结束格式错误";
                try {
                    int ur = Integer.parseInt(parts[1]);
                    DuelRoom uRoom = duelRooms.get(ur);
                    if (uRoom == null) return "ERROR|房间不存在";
                    synchronized (uRoom) {
                        if (uRoom.uno != null) unoEnd(uRoom, parts[2]);
                        return "SUCCESS";
                    }
                } catch (NumberFormatException e) { return "ERROR|参数格式错误"; }
            default:
                return "ERROR|未知操作: " + action;
        }
    }
}
