import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.*;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class WebChatServerPublic {

    // =========================================================
    // SERVER SETTINGS
    // =========================================================

    private static final int PORT =
        Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));

    private static final String WEBSOCKET_GUID =
            "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";


    // =========================================================
    // ORACLE DATABASE SETTINGS
    // =========================================================

    private static final String DB_URL =
            "jdbc:oracle:thin:@localhost:1521/orcl.mshome.net";

    private static final String DB_USER =
            "scott";

    private static final String DB_PASSWORD =
            "tiger";


    // =========================================================
    // ONLINE CLIENTS
    // =========================================================

    private static final Map<String, ClientHandler> clients =
            new ConcurrentHashMap<>();


    // =========================================================
    // CHAT HISTORY
    // =========================================================

    private static final List<ChatMessage> history =
            Collections.synchronizedList(new ArrayList<>());


    private static final DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("hh:mm a");


    // =========================================================
    // MAIN
    // =========================================================

    public static void main(String[] args) {

        System.out.println("======================================");
        System.out.println("       LIVE CHAT SERVER STARTED");
        System.out.println("======================================");
 System.out.println("Previous messages loaded: " + history.size());

        System.out.println();
        System.out.println("Open: http://localhost:" + PORT);
        System.out.println("Waiting for users...");
        System.out.println();


        try (ServerSocket serverSocket =
                     new ServerSocket(PORT)) {

            while (true) {

                Socket socket =
                        serverSocket.accept();

                System.out.println(
                        "New browser connection: "
                                + socket.getInetAddress()
                );

                new Thread(
                        new ClientHandler(socket)
                ).start();
            }

        } catch (Exception e) {

            e.printStackTrace();
        }
    }


    // =========================================================
    // DATABASE CONNECTION TEST
    // =========================================================

    private static boolean testDatabaseConnection() {

        try {

            Class.forName(
                    "oracle.jdbc.OracleDriver"
            );

            Connection con =
                    DriverManager.getConnection(
                            DB_URL,
                            DB_USER,
                            DB_PASSWORD
                    );

            con.close();

            return true;

        } catch (Exception e) {

            e.printStackTrace();

            return false;
        }
    }


    // =========================================================
    // GET DATABASE CONNECTION
    // =========================================================

    private static Connection getDatabaseConnection()
            throws SQLException {

        return DriverManager.getConnection(
                DB_URL,
                DB_USER,
                DB_PASSWORD
        );
    }


    // =========================================================
    // SAVE USER TO DATABASE
    // =========================================================

    private static void saveUserToDatabase(
            String username) {

        String sql =
                "INSERT INTO chat_users (username) VALUES (?)";

        try (Connection con =
                     getDatabaseConnection();

             PreparedStatement ps =
                     con.prepareStatement(sql)) {

            ps.setString(1, username);

            ps.executeUpdate();

            System.out.println(
                    "User saved to database: "
                            + username
            );

        } catch (SQLException e) {

            // Username may already exist because of UNIQUE constraint.
            // That is okay for this project.

            if (e.getErrorCode() == 1) {

                System.out.println(
                        "User already exists in database: "
                                + username
                );

            } else {

                System.out.println(
                        "Could not save user: "
                                + username
                );

                e.printStackTrace();
            }
        }
    }


    // =========================================================
    // SAVE MESSAGE TO DATABASE
    // =========================================================

    private static void saveMessageToDatabase(
            String username,
            String message) {

        String sql =
                "INSERT INTO chat_messages " +
                "(username, message_text) " +
                "VALUES (?, ?)";

        try (Connection con =
                     getDatabaseConnection();

             PreparedStatement ps =
                     con.prepareStatement(sql)) {

            ps.setString(1, username);
            ps.setString(2, message);

            ps.executeUpdate();

            System.out.println(
                    "Message saved to database."
            );

        } catch (SQLException e) {

            System.out.println(
                    "Could not save message."
            );

            e.printStackTrace();
        }
    }


    // =========================================================
    // LOAD PREVIOUS CHAT HISTORY
    // =========================================================

    private static void loadHistoryFromDatabase() {

        String sql =
                "SELECT username, message_text, sent_at " +
                "FROM (" +
                "SELECT username, message_text, sent_at " +
                "FROM chat_messages " +
                "ORDER BY message_id DESC" +
                ") " +
                "WHERE ROWNUM <= 100 " +
                "ORDER BY sent_at ASC";


        try (Connection con =
                     getDatabaseConnection();

             PreparedStatement ps =
                     con.prepareStatement(sql);

             ResultSet rs =
                     ps.executeQuery()) {


            synchronized (history) {

                history.clear();


                while (rs.next()) {

                    String username =
                            rs.getString("username");

                    String message =
                            rs.getString("message_text");

                    Timestamp timestamp =
                            rs.getTimestamp("sent_at");


                    String time;

                    if (timestamp != null) {

                        time =
                                timestamp
                                        .toLocalDateTime()
                                        .toLocalTime()
                                        .format(
                                                TIME_FORMAT
                                        );

                    } else {

                        time =
                                LocalTime.now()
                                        .format(
                                                TIME_FORMAT
                                        );
                    }


                    history.add(
                            new ChatMessage(
                                    username,
                                    message,
                                    time
                            )
                    );
                }
            }


            System.out.println(
                    "Previous messages loaded: "
                            + history.size()
            );


        } catch (SQLException e) {

            System.out.println(
                    "Could not load chat history."
            );

            e.printStackTrace();
        }
    }


    // =========================================================
    // CLIENT HANDLER
    // =========================================================

    static class ClientHandler implements Runnable {

        private final Socket socket;

        private InputStream input;

        private OutputStream output;

        private String username;

        private boolean webSocketConnected = false;


        ClientHandler(Socket socket) {

            this.socket = socket;
        }


        @Override
        public void run() {

            try {

                input =
                        socket.getInputStream();

                output =
                        socket.getOutputStream();


                BufferedReader reader =
                        new BufferedReader(
                                new InputStreamReader(
                                        input,
                                        StandardCharsets.UTF_8
                                )
                        );


                String firstLine =
                        reader.readLine();


                if (firstLine == null) {

                    closeConnection();

                    return;
                }


                // =================================================
                // HTTP HEADERS
                // =================================================

                Map<String, String> headers =
                        new HashMap<>();


                String line;


                while (
                        (line = reader.readLine()) != null
                                && !line.isEmpty()
                ) {

                    int colon =
                            line.indexOf(':');


                    if (colon > 0) {

                        String key =
                                line.substring(
                                        0,
                                        colon
                                )
                                        .trim()
                                        .toLowerCase();


                        String value =
                                line.substring(
                                        colon + 1
                                )
                                        .trim();


                        headers.put(
                                key,
                                value
                        );
                    }
                }


                // =================================================
                // WEBSOCKET REQUEST
                // =================================================

                if (
                        "websocket".equalsIgnoreCase(
                                headers.get("upgrade")
                        )
                ) {

                    performWebSocketHandshake(
                            headers
                    );

                    webSocketConnected = true;

                    handleWebSocket();


                } else {

                    sendWebPage();
                }


            } catch (Exception e) {

                if (username != null) {

                    System.out.println(
                            username +
                                    " disconnected."
                    );
                }


            } finally {

                closeConnection();
            }
        }


        // =========================================================
        // SEND WEB PAGE
        // =========================================================

        private void sendWebPage()
                throws IOException {

            byte[] content =
                    HTML_PAGE.getBytes(
                            StandardCharsets.UTF_8
                    );


            String response =
                    "HTTP/1.1 200 OK\r\n" +
                    "Content-Type: text/html; charset=UTF-8\r\n" +
                    "Content-Length: " +
                    content.length +
                    "\r\n" +
                    "Connection: close\r\n" +
                    "\r\n";


            output.write(
                    response.getBytes(
                            StandardCharsets.UTF_8
                    )
            );


            output.write(content);

            output.flush();

            socket.close();
        }


        // =========================================================
        // WEBSOCKET HANDSHAKE
        // =========================================================

        private void performWebSocketHandshake(
                Map<String, String> headers)
                throws Exception {


            String clientKey =
                    headers.get(
                            "sec-websocket-key"
                    );


            String acceptKey =
                    Base64.getEncoder().encodeToString(

                            MessageDigest
                                    .getInstance("SHA-1")
                                    .digest(
                                            (
                                                    clientKey +
                                                            WEBSOCKET_GUID
                                            )
                                                    .getBytes(
                                                            StandardCharsets.UTF_8
                                                    )
                                    )
                    );


            String response =
                    "HTTP/1.1 101 Switching Protocols\r\n" +
                    "Upgrade: websocket\r\n" +
                    "Connection: Upgrade\r\n" +
                    "Sec-WebSocket-Accept: " +
                    acceptKey +
                    "\r\n" +
                    "\r\n";


            output.write(
                    response.getBytes(
                            StandardCharsets.UTF_8
                    )
            );


            output.flush();
        }


        // =========================================================
        // WEBSOCKET LOOP
        // =========================================================

        private void handleWebSocket()
                throws Exception {


            while (true) {

                WebSocketFrame frame =
                        readFrame();


                if (frame == null) {

                    break;
                }


                // TEXT
                if (frame.opcode == 1) {

                    String message =
                            frame.text;

                    processMessage(message);
                }


                // CLOSE
                else if (frame.opcode == 8) {

                    break;
                }


                // PING
                else if (frame.opcode == 9) {

                    sendFrame(
                            (byte) 10,
                            ""
                    );
                }
            }
        }


        // =========================================================
        // PROCESS MESSAGE
        // =========================================================

        private void processMessage(
                String message) {


            try {


                // =================================================
                // JOIN
                // =================================================

                if (message.startsWith("JOIN|")) {


                    String name =
                            message.substring(5)
                                    .trim();


                    if (name.isEmpty()) {

                        return;
                    }


                    name =
                            URLDecoder.decode(
                                    name,
                                    StandardCharsets.UTF_8
                            );


                    if (name.length() > 20) {

                        name =
                                name.substring(
                                        0,
                                        20
                                );
                    }


                    // =================================================
                    // DUPLICATE ONLINE USER
                    // =================================================

                    if (clients.containsKey(name)) {

                        sendFrame(
                                (byte) 1,
                                "ERROR|Username already taken"
                        );

                        return;
                    }


                    username = name;


                    clients.put(
                            username,
                            this
                    );


                    System.out.println(
                            username +
                                    " joined the chat."
                    );


                    // =================================================
                    // SAVE USER
                    // =================================================

                     //saveUserToDatabase( username );


                    // =================================================
                    // SEND PREVIOUS MESSAGES
                    // =================================================

                    synchronized (history) {

                        for (
                                ChatMessage chat :
                                history
                        ) {

                            sendFrame(
                                    (byte) 1,
                                    "CHAT|" +
                                            encode(
                                                    chat.username
                                            ) +
                                            "|" +
                                            encode(
                                                    chat.message
                                            ) +
                                            "|" +
                                            chat.time
                            );
                        }
                    }


                    // =================================================
                    // JOIN NOTIFICATION
                    // =================================================

                    broadcast(
                            "SYSTEM|" +
                                    encode(username) +
                                    " joined the chat."
                    );


                    // =================================================
                    // UPDATE USERS
                    // =================================================

                    broadcastUsers();
                }


                // =================================================
                // CHAT MESSAGE
                // =================================================

                else if (
                        message.startsWith("CHAT|")
                ) {


                    if (username == null) {

                        return;
                    }


                    String encodedMessage =
                            message.substring(5);


                    String chatMessage =
                            URLDecoder.decode(
                                    encodedMessage,
                                    StandardCharsets.UTF_8
                            );


                    if (
                            chatMessage
                                    .trim()
                                    .isEmpty()
                    ) {

                        return;
                    }


                    if (
                            chatMessage.length() > 500
                    ) {

                        chatMessage =
                                chatMessage.substring(
                                        0,
                                        500
                                );
                    }


                    String time =
                            LocalTime.now()
                                    .format(
                                            TIME_FORMAT
                                    );


                    ChatMessage chat =
                            new ChatMessage(
                                    username,
                                    chatMessage,
                                    time
                            );


                    // =================================================
                    // SAVE TO MEMORY
                    // =================================================

                    history.add(chat);


                    if (history.size() > 100) {

                        history.remove(0);
                    }


                    // =================================================
                    // SAVE TO ORACLE
                    // =================================================

                  //  saveMessageToDatabase( username  chatMessage );


                    // =================================================
                    // BROADCAST
                    // =================================================

                    broadcast(
                            "CHAT|" +
                                    encode(username) +
                                    "|" +
                                    encode(chatMessage) +
                                    "|" +
                                    time
                    );
                }


                // =================================================
                // TYPING
                // =================================================

                else if (
                        message.startsWith("TYPING|")
                ) {


                    if (username == null) {

                        return;
                    }


                    String value =
                            message.substring(7);


                    broadcastExceptSelf(
                            "TYPING|" +
                                    encode(username) +
                                    "|" +
                                    value
                    );
                }


            } catch (Exception e) {

                e.printStackTrace();
            }
        }


        // =========================================================
        // BROADCAST
        // =========================================================

        private static void broadcast(
                String message) {


            for (
                    ClientHandler client :
                    clients.values()
            ) {

                try {

                    client.sendFrame(
                            (byte) 1,
                            message
                    );

                } catch (Exception ignored) {
                }
            }
        }


        // =========================================================
        // BROADCAST EXCEPT SELF
        // =========================================================

        private void broadcastExceptSelf(
                String message) {


            for (
                    ClientHandler client :
                    clients.values()
            ) {

                if (client != this) {

                    try {

                        client.sendFrame(
                                (byte) 1,
                                message
                        );

                    } catch (Exception ignored) {
                    }
                }
            }
        }


        // =========================================================
        // USERS LIST
        // =========================================================

        private static void broadcastUsers() {


            StringBuilder users =
                    new StringBuilder();


            for (
                    String name :
                    clients.keySet()
            ) {


                if (users.length() > 0) {

                    users.append(",");
                }


                users.append(
                        encode(name)
                );
            }


            broadcast(
                    "USERS|" +
                            clients.size() +
                            "|" +
                            users
            );
        }


        // =========================================================
        // READ WEBSOCKET FRAME
        // =========================================================

        private WebSocketFrame readFrame()
                throws IOException {


            int firstByte =
                    input.read();


            if (firstByte == -1) {

                return null;
            }


            int secondByte =
                    input.read();


            if (secondByte == -1) {

                return null;
            }


            int opcode =
                    firstByte & 0x0F;


            boolean masked =
                    (secondByte & 0x80) != 0;


            int payloadLength =
                    secondByte & 0x7F;


            if (payloadLength == 126) {


                payloadLength =
                        (input.read() << 8)
                                | input.read();
            }


            else if (payloadLength == 127) {


                long length = 0;


                for (int i = 0; i < 8; i++) {

                    length =
                            (length << 8)
                                    | input.read();
                }


                if (
                        length >
                                Integer.MAX_VALUE
                ) {

                    throw new IOException(
                            "Message too large"
                    );
                }


                payloadLength =
                        (int) length;
            }


            byte[] mask = null;


            if (masked) {


                mask =
                        new byte[4];


                readFully(mask);
            }


            byte[] payload =
                    new byte[payloadLength];


            readFully(payload);


            if (masked) {


                for (
                        int i = 0;
                        i < payload.length;
                        i++
                ) {

                    payload[i] =
                            (byte) (
                                    payload[i]
                                            ^
                                            mask[
                                                    i % 4
                                            ]
                            );
                }
            }


            String text =
                    new String(
                            payload,
                            StandardCharsets.UTF_8
                    );


            return new WebSocketFrame(
                    opcode,
                    text
            );
        }


        // =========================================================
        // READ FULL BYTE ARRAY
        // =========================================================

        private void readFully(
                byte[] data)
                throws IOException {


            int total = 0;


            while (
                    total <
                            data.length
            ) {


                int count =
                        input.read(
                                data,
                                total,
                                data.length - total
                        );


                if (count == -1) {

                    throw new EOFException();
                }


                total += count;
            }
        }


        // =========================================================
        // SEND WEBSOCKET FRAME
        // =========================================================

        private synchronized void sendFrame(
                byte opcode,
                String message) {


            try {


                byte[] data =
                        message.getBytes(
                                StandardCharsets.UTF_8
                        );


                int length =
                        data.length;


                ByteArrayOutputStream frame =
                        new ByteArrayOutputStream();


                frame.write(
                        0x80 |
                                (opcode & 0x0F)
                );


                if (length <= 125) {

                    frame.write(length);

                }


                else if (length <= 65535) {

                    frame.write(126);

                    frame.write(
                            (length >> 8)
                                    & 0xFF
                    );

                    frame.write(
                            length & 0xFF
                    );

                }


                else {

                    frame.write(127);


                    long longLength =
                            length;


                    for (
                            int i = 7;
                            i >= 0;
                            i--
                    ) {

                        frame.write(
                                (int) (
                                        (
                                                longLength
                                                        >> (
                                                        8 * i
                                                )
                                        )
                                                & 0xFF
                                )
                        );
                    }
                }


                frame.write(data);


                output.write(
                        frame.toByteArray()
                );


                output.flush();


            } catch (Exception ignored) {
            }
        }


        // =========================================================
        // CLOSE CONNECTION
        // =========================================================

        private void closeConnection() {


            try {


                if (username != null) {


                    String leavingUser =
                            username;


                    clients.remove(
                            username
                    );


                    System.out.println(
                            leavingUser +
                                    " left the chat."
                    );


                    broadcast(
                            "SYSTEM|" +
                                    encode(
                                            leavingUser
                                    ) +
                                    " left the chat."
                    );


                    broadcastUsers();


                    username = null;
                }


                if (
                        socket != null
                                &&
                                !socket.isClosed()
                ) {

                    socket.close();
                }


            } catch (Exception ignored) {
            }
        }
    }


    // =========================================================
    // WEBSOCKET FRAME CLASS
    // =========================================================

    static class WebSocketFrame {

        int opcode;

        String text;


        WebSocketFrame(
                int opcode,
                String text) {

            this.opcode = opcode;

            this.text = text;
        }
    }


    // =========================================================
    // CHAT MESSAGE CLASS
    // =========================================================

    static class ChatMessage {

        String username;

        String message;

        String time;


        ChatMessage(
                String username,
                String message,
                String time) {

            this.username = username;

            this.message = message;

            this.time = time;
        }
    }


    // =========================================================
    // URL ENCODE
    // =========================================================

    private static String encode(
            String text) {


        try {


            return URLEncoder.encode(
                    text,
                    StandardCharsets.UTF_8
            )
                    .replace(
                            "+",
                            "%20"
                    );


        } catch (Exception e) {

            return text;
        }
    }


    // =========================================================
    // HTML + CSS + JAVASCRIPT
    // =========================================================

    private static final String HTML_PAGE = """
<!DOCTYPE html>
<html lang="en">

<head>

<meta charset="UTF-8">

<meta name="viewport"
      content="width=device-width, initial-scale=1.0">

<title>LiveChat</title>

<style>

* {
    box-sizing: border-box;
    margin: 0;
    padding: 0;
}

body {
    font-family: Arial, Helvetica, sans-serif;
    background: #0b1120;
    color: white;
    height: 100vh;
    overflow: hidden;
}


/* =====================================================
   LOGIN SCREEN
   ===================================================== */

#loginScreen {
    height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;

    background:
        radial-gradient(
            circle at top,
            #1e293b,
            #0b1120 60%
        );
}


.loginBox {
    width: 400px;
    max-width: 90%;

    padding: 40px;

    border-radius: 24px;

    background:
        rgba(17, 24, 39, 0.95);

    border:
        1px solid
        rgba(255,255,255,0.08);

    box-shadow:
        0 25px 70px
        rgba(0,0,0,0.5);

    text-align: center;
}


.logo {
    font-size: 42px;
    margin-bottom: 10px;
}


.loginBox h1 {
    font-size: 30px;
    margin-bottom: 8px;
}


.loginBox p {
    color: #94a3b8;
    margin-bottom: 28px;
}


.loginBox input {
    width: 100%;

    padding: 15px;

    border-radius: 12px;

    border:
        1px solid #334155;

    background: #0f172a;

    color: white;

    outline: none;

    font-size: 16px;

    margin-bottom: 15px;
}


.loginBox input:focus {
    border-color: #6366f1;
}


.loginBox button {
    width: 100%;

    padding: 15px;

    border: none;

    border-radius: 12px;

    background: #6366f1;

    color: white;

    font-size: 16px;

    font-weight: bold;

    cursor: pointer;
}


.loginBox button:hover {
    background: #4f46e5;
}


/* =====================================================
   CHAT APPLICATION
   ===================================================== */

#chatScreen {
    display: none;

    height: 100vh;

    flex-direction: column;
}


.topBar {
    height: 70px;

    display: flex;

    align-items: center;

    justify-content: space-between;

    padding: 0 25px;

    background: #111827;

    border-bottom:
        1px solid #1f2937;
}


.brand {
    display: flex;

    align-items: center;

    gap: 12px;
}


.brandIcon {
    width: 42px;
    height: 42px;

    border-radius: 12px;

    background: #6366f1;

    display: flex;

    align-items: center;

    justify-content: center;

    font-size: 21px;
}


.brandText h2 {
    font-size: 18px;
}


.brandText span {
    font-size: 12px;

    color: #94a3b8;
}


.topRight {
    display: flex;

    align-items: center;

    gap: 15px;
}


.connection {
    display: flex;

    align-items: center;

    gap: 7px;

    font-size: 13px;

    color: #86efac;
}


.statusDot {
    width: 9px;
    height: 9px;

    border-radius: 50%;

    background: #22c55e;
}


.logoutBtn {
    border:
        1px solid #374151;

    background: #1f2937;

    color: #f87171;

    padding: 9px 15px;

    border-radius: 10px;

    cursor: pointer;

    font-weight: bold;
}


.logoutBtn:hover {
    background: #374151;
}


/* =====================================================
   MAIN AREA
   ===================================================== */

.mainArea {
    flex: 1;

    display: flex;

    min-height: 0;
}


/* =====================================================
   CHAT AREA
   ===================================================== */

.chatArea {
    flex: 1;

    display: flex;

    flex-direction: column;

    min-width: 0;
}


.messages {
    flex: 1;

    overflow-y: auto;

    padding: 25px;
}


.messageRow {
    display: flex;

    margin-bottom: 14px;
}


.messageRow.mine {
    justify-content: flex-end;
}


.messageBubble {
    max-width: 70%;

    padding: 11px 15px;

    border-radius: 16px;

    background: #1e293b;
}


.messageRow.mine .messageBubble {
    background: #4f46e5;
}


.sender {
    font-size: 12px;

    color: #a5b4fc;

    margin-bottom: 5px;

    font-weight: bold;
}


.messageRow.mine .sender {
    color: #ddd6fe;
}


.messageText {
    font-size: 15px;

    line-height: 1.4;

    word-wrap: break-word;
}


.time {
    font-size: 10px;

    color: #94a3b8;

    margin-top: 5px;

    text-align: right;
}


.messageRow.mine .time {
    color: #c7d2fe;
}


.systemMessage {
    text-align: center;

    color: #64748b;

    font-size: 12px;

    margin: 15px 0;
}


/* =====================================================
   TYPING
   ===================================================== */

.typingArea {
    height: 25px;

    padding-left: 25px;

    color: #94a3b8;

    font-size: 12px;
}


/* =====================================================
   MESSAGE INPUT
   ===================================================== */

.inputArea {
    padding:
        15px 20px 20px;

    display: flex;

    gap: 10px;

    border-top:
        1px solid #1f2937;

    background: #111827;
}


.inputArea input {
    flex: 1;

    padding: 14px;

    border-radius: 12px;

    border:
        1px solid #334155;

    background: #0f172a;

    color: white;

    outline: none;

    font-size: 15px;
}


.inputArea input:focus {
    border-color: #6366f1;
}


.sendBtn {
    width: 55px;

    border: none;

    border-radius: 12px;

    background: #6366f1;

    color: white;

    font-size: 20px;

    cursor: pointer;
}


.sendBtn:hover {
    background: #4f46e5;
}


/* =====================================================
   ONLINE USERS
   ===================================================== */

.onlinePanel {
    width: 280px;

    border-left:
        1px solid #1f2937;

    background: #111827;

    padding: 25px;

    overflow-y: auto;
}


.onlineHeader {
    display: flex;

    align-items: center;

    justify-content: space-between;

    margin-bottom: 20px;
}


.onlineHeader h3 {
    font-size: 16px;
}


.liveBadge {
    font-size: 10px;

    padding: 5px 8px;

    border-radius: 20px;

    background:
        rgba(34,197,94,0.12);

    color: #4ade80;
}


.onlineCount {
    font-size: 50px;

    font-weight: bold;

    margin-bottom: 5px;
}


.onlineText {
    color: #64748b;

    font-size: 13px;

    margin-bottom: 25px;
}


.userList {
    display: flex;

    flex-direction: column;

    gap: 12px;
}


.userItem {
    display: flex;

    align-items: center;

    gap: 10px;
}


.avatar {
    width: 38px;
    height: 38px;

    border-radius: 50%;

    background: #312e81;

    display: flex;

    align-items: center;

    justify-content: center;

    font-weight: bold;
}


.userName {
    font-size: 14px;
}


.userStatus {
    color: #4ade80;

    font-size: 10px;
}


/* =====================================================
   MOBILE
   ===================================================== */

@media (max-width: 750px) {

    .onlinePanel {
        display: none;
    }

    .messageBubble {
        max-width: 85%;
    }

    .topBar {
        padding: 0 15px;
    }

    .connection {
        display: none;
    }
}

</style>

</head>


<body>


<!-- =====================================================
     LOGIN
     ===================================================== -->

<div id="loginScreen">

    <div class="loginBox">

        <div class="logo">💬</div>

        <h1>LiveChat</h1>

        <p>Connect and chat in real time</p>

        <input
            id="usernameInput"
            type="text"
            maxlength="20"
            placeholder="Enter your username"
            autocomplete="off"
        >

        <button onclick="joinChat()">
            Join Chat
        </button>

    </div>

</div>


<!-- =====================================================
     CHAT SCREEN
     ===================================================== -->

<div id="chatScreen">

    <div class="topBar">

        <div class="brand">

            <div class="brandIcon">
                💬
            </div>

            <div class="brandText">

                <h2>LiveChat</h2>

                <span id="loggedUser">
                    Not connected
                </span>

            </div>

        </div>


        <div class="topRight">

            <div class="connection">

                <div class="statusDot"></div>

                <span>
                    Connected
                </span>

            </div>

            <button
                class="logoutBtn"
                onclick="logout()"
            >
                Logout
            </button>

        </div>

    </div>


    <div class="mainArea">


        <!-- CHAT -->

        <div class="chatArea">

            <div
                id="messages"
                class="messages"
            ></div>


            <div
                id="typingArea"
                class="typingArea"
            ></div>


            <div class="inputArea">

                <input
                    id="messageInput"
                    type="text"
                    maxlength="500"
                    placeholder="Type a message..."
                    autocomplete="off"
                >


                <button
                    class="sendBtn"
                    onclick="sendMessage()"
                >
                    ➤
                </button>

            </div>

        </div>


        <!-- ONLINE PANEL -->

        <div class="onlinePanel">

            <div class="onlineHeader">

                <h3>Online Users</h3>

                <span class="liveBadge">
                    ● LIVE
                </span>

            </div>


            <div
                id="onlineCount"
                class="onlineCount"
            >
                0
            </div>


            <div class="onlineText">
                people online
            </div>


            <div
                id="userList"
                class="userList"
            ></div>

        </div>

    </div>

</div>


<script>

let socket = null;

let username = "";

let typingTimer = null;

let currentlyTyping = new Set();


// =====================================================
// JOIN CHAT
// =====================================================

function joinChat() {

    const input =
        document.getElementById(
            "usernameInput"
        );


    username =
        input.value.trim();


    if (!username) {

        alert(
            "Please enter a username."
        );

        return;
    }


    connectWebSocket();
}


// =====================================================
// CONNECT WEBSOCKET
// =====================================================

function connectWebSocket() {

     socket = new WebSocket(
    (location.protocol === "https:" ? "wss://" : "ws://") + location.host
);

    socket.onopen = function() {


        socket.send(
            "JOIN|" +
            encodeURIComponent(
                username
            )
        );


        document.getElementById(
            "loginScreen"
        ).style.display =
            "none";


        document.getElementById(
            "chatScreen"
        ).style.display =
            "flex";


        document.getElementById(
            "loggedUser"
        ).textContent =
            "Logged in as " +
            username;


        document.getElementById(
            "messageInput"
        ).focus();
    };


    socket.onmessage = function(event) {

        handleServerMessage(
            event.data
        );
    };


    socket.onclose = function() {

        console.log(
            "Connection closed."
        );
    };


    socket.onerror = function() {

        alert(
            "Could not connect to server."
        );
    };
}


// =====================================================
// SERVER MESSAGE
// =====================================================

function handleServerMessage(data) {

    const parts =
        data.split("|");


    const type =
        parts[0];


    // ERROR

    if (type === "ERROR") {

        alert(
            decodeURIComponent(
                parts
                    .slice(1)
                    .join("|")
            )
        );


        if (socket) {

            socket.close();
        }


        return;
    }


    // CHAT

    if (type === "CHAT") {

        const sender =
            decodeURIComponent(
                parts[1]
            );


        const message =
            decodeURIComponent(
                parts[2]
            );


        const time =
            parts[3];


        addMessage(
            sender,
            message,
            time
        );


        return;
    }


    // SYSTEM

    if (type === "SYSTEM") {

        const message =
            decodeURIComponent(
                parts
                    .slice(1)
                    .join("|")
            );


        addSystemMessage(
            message
        );


        return;
    }


    // USERS

    if (type === "USERS") {

        const count =
            parts[1];


        const users =
            parts[2]
                ? parts[2]
                    .split(",")
                    .filter(Boolean)
                : [];


        updateUsers(
            count,
            users
        );


        return;
    }


    // TYPING

    if (type === "TYPING") {

        const sender =
            decodeURIComponent(
                parts[1]
            );


        const isTyping =
            parts[2] === "1";


        if (isTyping) {

            currentlyTyping.add(
                sender
            );

        } else {

            currentlyTyping.delete(
                sender
            );
        }


        updateTyping();


        return;
    }
}


// =====================================================
// SEND MESSAGE
// =====================================================

function sendMessage() {

    const input =
        document.getElementById(
            "messageInput"
        );


    const message =
        input.value.trim();


    if (!message) {

        return;
    }


    if (
        !socket ||
        socket.readyState !==
            WebSocket.OPEN
    ) {

        return;
    }


    socket.send(
        "CHAT|" +
        encodeURIComponent(
            message
        )
    );


    input.value = "";


    sendTyping(false);


    input.focus();
}


// =====================================================
// ADD MESSAGE
// =====================================================

function addMessage(
    sender,
    message,
    time
) {


    const container =
        document.getElementById(
            "messages"
        );


    const row =
        document.createElement(
            "div"
        );


    const mine =
        sender === username;


    row.className =
        "messageRow " +
        (mine ? "mine" : "");


    const bubble =
        document.createElement(
            "div"
        );


    bubble.className =
        "messageBubble";


    const senderElement =
        document.createElement(
            "div"
        );


    senderElement.className =
        "sender";


    senderElement.textContent =
        mine
            ? "You"
            : sender;


    const messageElement =
        document.createElement(
            "div"
        );


    messageElement.className =
        "messageText";


    messageElement.textContent =
        message;


    const timeElement =
        document.createElement(
            "div"
        );


    timeElement.className =
        "time";


    timeElement.textContent =
        time;


    bubble.appendChild(
        senderElement
    );


    bubble.appendChild(
        messageElement
    );


    bubble.appendChild(
        timeElement
    );


    row.appendChild(
        bubble
    );


    container.appendChild(
        row
    );


    container.scrollTop =
        container.scrollHeight;
}


// =====================================================
// SYSTEM MESSAGE
// =====================================================

function addSystemMessage(
    message
) {


    const container =
        document.getElementById(
            "messages"
        );


    const element =
        document.createElement(
            "div"
        );


    element.className =
        "systemMessage";


    element.textContent =
        message;


    container.appendChild(
        element
    );


    container.scrollTop =
        container.scrollHeight;
}


// =====================================================
// UPDATE ONLINE USERS
// =====================================================

function updateUsers(
    count,
    users
) {


    document.getElementById(
        "onlineCount"
    ).textContent =
        count;


    const list =
        document.getElementById(
            "userList"
        );


    list.innerHTML = "";


    users.forEach(
        function(encodedName) {


            const name =
                decodeURIComponent(
                    encodedName
                );


            const item =
                document.createElement(
                    "div"
                );


            item.className =
                "userItem";


            const avatar =
                document.createElement(
                    "div"
                );


            avatar.className =
                "avatar";


            avatar.textContent =
                name
                    .charAt(0)
                    .toUpperCase();


            const info =
                document.createElement(
                    "div"
                );


            const nameElement =
                document.createElement(
                    "div"
                );


            nameElement.className =
                "userName";


            nameElement.textContent =
                name;


            const status =
                document.createElement(
                    "div"
                );


            status.className =
                "userStatus";


            status.textContent =
                "● Online";


            info.appendChild(
                nameElement
            );


            info.appendChild(
                status
            );


            item.appendChild(
                avatar
            );


            item.appendChild(
                info
            );


            list.appendChild(
                item
            );
        }
    );
}


// =====================================================
// TYPING
// =====================================================

function sendTyping(
    isTyping
) {


    if (
        !socket ||
        socket.readyState !==
            WebSocket.OPEN
    ) {

        return;
    }


    socket.send(
        "TYPING|" +
        (isTyping ? "1" : "0")
    );
}


function updateTyping() {


    const area =
        document.getElementById(
            "typingArea"
        );


    const names =
        Array.from(
            currentlyTyping
        );


    if (
        names.length === 0
    ) {

        area.textContent = "";


    } else if (
        names.length === 1
    ) {

        area.textContent =
            names[0] +
            " is typing...";


    } else {

        area.textContent =
            names.join(", ") +
            " are typing...";
    }
}


// =====================================================
// LOGOUT
// =====================================================

function logout() {


    if (socket) {

        try {

            socket.close();

        } catch (e) {
        }


        socket = null;
    }


    currentlyTyping.clear();


    document.getElementById(
        "messages"
    ).innerHTML = "";


    document.getElementById(
        "userList"
    ).innerHTML = "";


    document.getElementById(
        "onlineCount"
    ).textContent = "0";


    document.getElementById(
        "typingArea"
    ).textContent = "";


    document.getElementById(
        "messageInput"
    ).value = "";


    document.getElementById(
        "usernameInput"
    ).value =
        username;


    document.getElementById(
        "chatScreen"
    ).style.display =
        "none";


    document.getElementById(
        "loginScreen"
    ).style.display =
        "flex";


    username = "";


    document.getElementById(
        "usernameInput"
    ).focus();
}


// =====================================================
// ENTER TO SEND
// =====================================================

document.getElementById(
    "messageInput"
).addEventListener(
    "keydown",
    function(event) {


        if (
            event.key === "Enter"
        ) {

            event.preventDefault();

            sendMessage();
        }
    }
);


// =====================================================
// TYPING DETECTION
// =====================================================

document.getElementById(
    "messageInput"
).addEventListener(
    "input",
    function() {


        sendTyping(true);


        clearTimeout(
            typingTimer
        );


        typingTimer =
            setTimeout(
                function() {

                    sendTyping(false);

                },
                1000
            );
    }
);


// =====================================================
// ENTER TO JOIN
// =====================================================

document.getElementById(
    "usernameInput"
).addEventListener(
    "keydown",
    function(event) {


        if (
            event.key === "Enter"
        ) {

            event.preventDefault();

            joinChat();
        }
    }
);


// =====================================================
// CLOSE SOCKET WHEN PAGE CLOSES
// =====================================================

window.addEventListener(
    "beforeunload",
    function() {


        if (socket) {

            socket.close();
        }
    }
);

</script>

</body>

</html>
""";
}
