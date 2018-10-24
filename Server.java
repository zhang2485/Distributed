import java.io.*;
import java.net.*;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.nio.file.Files;

public class Server {
    static final String IP_DELIMITER = " ";
    private static final String INTRODUCER_IP = "172.22.156.255";
    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss.SSS");
    private static final int SERVER_PORT = 2017;
    volatile static String ip;
    volatile static ArrayList<String> group = new ArrayList<>();
    private volatile static HashMap<String, String> connectTimes = new HashMap<>();
    private static ServerSocket serverSocket;
    private static String machine;
    private static FileWriter log;

    private static ArrayList<String> removeDuplicatesAndSort(ArrayList<String> list) {
        Set<String> set = new HashSet<>(list);
        ArrayList<String> ret = new ArrayList<>(set);
        Collections.sort(ret);
        return ret;
    }

    private static ArrayList<String> logGrep(String pattern, String logFileName) {
        ArrayList<String> res = null;
        try {
            pattern = pattern.trim();
            logFileName = logFileName.trim();
            // run Linux command line in java program
            Runtime rt = Runtime.getRuntime();
            // Set the actual command based on feeding pattern
            String[] cmd = {"/bin/sh", "-c", String.format("grep %s %s", pattern, logFileName).trim()};
            // Run command line
            Process proc = rt.exec(cmd);
            // Convert the command result into a String List
            return saveStream(proc.getInputStream());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return res;
    }

    private static ArrayList<String> saveStream(InputStream in) throws IOException {
        // Create a new bufferedReader using inputStream based on running result of the command line
        BufferedReader is = new BufferedReader(new InputStreamReader(in));
        ArrayList<String> arr = new ArrayList<>();
        String line;
        // Loop through each line of the running result
        while ((line = is.readLine()) != null) {
            // Append each line into the String list
            arr.add(line);
        }
        // Return a list of String as running result
        return arr;
    }

    private static String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    private static String getCanonicalName() throws UnknownHostException {
        return InetAddress.getLocalHost().getCanonicalHostName();
    }

    private static String getIPAddress() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostAddress();
    }

    private static String getLogFileName() {
        return String.format("%s.log", machine);
    }

    public static String getCurrentDateAsString() {
        return dateFormatter.format(new Date());
    }

    static void writeToLog(String msg) throws IOException {
        String date = getCurrentDateAsString();
        String constructedLog = String.format("%s: %s\n", date, msg);
        System.out.print(constructedLog);
        log.write(constructedLog);
        log.flush();
    }

    static void addToMemberList(String ip) throws IOException {
        group.add(ip);
        group = removeDuplicatesAndSort(group);
        String date = getCurrentDateAsString();
        connectTimes.put(ip, date);
        writeToLog(String.format("Added %s to member list", ip));
        logMemberList();
    }

    static void removeFromMemberList(String ip) throws IOException {
        int found = Server.group.indexOf(ip);
        if (found != -1) {
            Server.group.remove(found);
            writeToLog(String.format("Removed %s from member list", ip));
        } else {
            writeToLog(String.format("Requested removal of members list for %s but it wasn't there", ip));
        }
        logMemberList();
    }

    static void updateMemberList(String[] newList) throws IOException {
        updateMemberList(new ArrayList<>(Arrays.asList(newList)));
    }

    private static void updateMemberList(ArrayList<String> newList) throws IOException {
        writeToLog("Received update by membership list");
        group = removeDuplicatesAndSort(newList);
        for (int i = 0; i < Server.group.size(); i++) {
            String ip = Server.group.get(i);
            String date = getCurrentDateAsString();
            connectTimes.put(ip, date);
        }
        logMemberList();
    }

    private static void logMemberList() throws IOException {
        writeToLog(String.format("New member list: %s", Server.group.toString()));
    }

    static String memberListToPacketData() {
        return String.join(IP_DELIMITER, Server.group);
    }

    private static void setupServer() throws IOException {
        // Must collect metadata about computer before creating log file
        machine = getCanonicalName();
        ip = getIPAddress();
        // FileWriter object contains instance to log file for server
        log = new FileWriter(new File(getLogFileName()));
        writeToLog(String.format("Attempting to start server at: %s", ip));

        if (ip.equals(INTRODUCER_IP)) {
            // If I am the introducer machine
            addToMemberList(ip);
            new AckThread().start();
            new IntroducerThread().start();
            new ConnectThread().start();
            new PingThread().start();
        } else {
            /*
             *  The following section checks if the introducer machine is alive by pinging it,
             *  waiting for its response, and then taking the respective action. The ping holds the ip address
             *  of the machine that is pinging so that the introducer can disseminate the ip.
             *
             *  If the introducer machine is available:
             *      start a new server
             *  else
             *      exit
             */
            SocketHelper socket = new SocketHelper(SocketHelper.CONNECT_PORT);
            socket.send(ip, INTRODUCER_IP, SocketHelper.INTRODUCER_PORT);
            try {
                DatagramPacket packet = socket.receive(MyThread.PROTOCOL_PERIOD);

                // We have successfully confirmed introducer is available
                socket.close(); // Allow connect thread to open port at CONNECT_PORT
                String msg = SocketHelper.getStringFromPacket(packet);
                updateMemberList(msg.split(Server.IP_DELIMITER));
                new AckThread().start();
                new ConnectThread().start();
                new PingThread().start();
            } catch (SocketTimeoutException e) {
                //Introducer is inactive
                writeToLog("Introducer is inactive");
                System.exit(0);
            }
        }
    }

    public static void main(String args[]) throws IOException, InterruptedException {
        setupServer();
        // Now that the server is setup, we can handle inputs from the client for commands
        String cmd;
        String[] cmds;
        serverSocket = new ServerSocket(SERVER_PORT);
        while (true) {
            writeToLog("Looking for new connection on server");
            Socket acceptSocket = serverSocket.accept();
            BufferedReader reader = new BufferedReader(new InputStreamReader(acceptSocket.getInputStream()));
            DataOutputStream writer = new DataOutputStream(acceptSocket.getOutputStream());
            cmd = reader.readLine();
            cmds = cmd.split(" ");
            try {
                switch (cmds[0]) {
                    /*
                     *  print:
                     *  This command prints out the current membership list on every available server
                     */
                    case "print":
                        writer.writeBytes(String.format("My ID is: %s\n", ip));
                        writer.writeBytes(String.format("%d members are in my group\n", Server.group.size()));
                        for (String server : Server.group)
                            writer.writeBytes(String.format("%s\n", server));
                        writeToLog("Sent membership list to client");
                        break;
                    /*
                     *  grep:
                     *  execute a grep command on the log file and send back the grep results
                     */
                    case "grep":
                        if (cmds.length < 2) {
                            writeToLog("grep command did not have the right arguments");
                            continue;
                        }
                        // Extract pattern
                        int patternStart = cmd.indexOf(" ");
                        ArrayList<String> res = logGrep(cmd.substring(patternStart + 1), getLogFileName());
                        writer.writeBytes(String.join("\n", res));
                        writeToLog("Sent grep output to client");
                        break;
                    /*
                     *  quit:
                     *  exits if I was commanded to quit, else update my membership list
                     */
                    case "quit":
                        if (cmds.length < 2) {
                            writeToLog("quit command did not have the right arguments");
                            continue;
                        }
                        String quitter = cmds[1];
                        if (ip.equals(quitter)) {
                            System.exit(0);
                        } else {
                            removeFromMemberList(quitter);
                        }
                        Server.writeToLog(String.format("%s naturally exited.", quitter));
                        break;
                    /*
                    log:
                    sends back the log of a specific ip address
                     */
                    case "log":
                        if (cmds.length < 2) {
                            writeToLog("log command did not have the right arguments");
                            continue;
                        }
                        if (ip.equals(cmds[1]) || machine.equals(cmds[1])) {
                            String fileContents = readFile(getLogFileName(), Charset.forName("UTF-8"));
                            writer.writeBytes(fileContents);
                        }
                        break;
                    /*
                     ls:
                     checks if file exists on VM
                     */
                    case "ls":
                        if (FileHandler.fileExists(cmds[1])) {
                            writer.writeBytes("File found!");
                        } else {
                            writer.writeBytes("File not found...");
                        }
                        writeToLog(String.format("Checked for %s.", cmds[1]));
                        break;
                    /*
                    put:
                    receives a file from the client
                     */
                    case "put":
                        writeToLog(String.format("Received put command: '%s'", cmd));
                        FileHandler.receiveFile(cmds[2], acceptSocket);

                        // Send ACK back
                        writer.writeBytes("Successfully received file\n");
                        Server.writeToLog("Sent ACK back to sender");
                        break;
                    default:
                        writeToLog(String.format("Received invalid command: %s", cmds[0]));
                        break;
                }
                acceptSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

class MyThread extends Thread {
    public static final int[] neighbors = {-2, -1, 1, 2};
    static final int PROTOCOL_PERIOD = 400;
    static final int FAIL_TIME = 1000;
    SocketHelper socket;

    int indexWrap(int idx) {
        if (idx < 0) {
            return Server.group.size() + idx;
        }
        return idx;
    }

    /*
     Since we can't use Thread.sleep because it is a static sleep that only sleeps
     the currently running main thread, we will use the system clock for a sleeping
     */
    void systemClockSleep(int millis) {
        Date start = new Date();
        while ((new Date().getTime() - start.getTime()) < millis) ;
    }

    void disseminate() throws IOException {
        Server.writeToLog("Disseminating my list: " + Server.group.toString());
        for (int i = 0; i < Server.group.size(); i++) {
            String currentIP = Server.group.get(i);
            if (!currentIP.equals(Server.ip)) {
                socket.send(Server.memberListToPacketData(), currentIP, SocketHelper.CONNECT_PORT);
            }
        }
    }

}

class AckThread extends MyThread implements Runnable {
    @Override
    public void run() {
        try {
            socket = new SocketHelper(SocketHelper.ACK_PORT);
            while (true) {
                DatagramPacket packet = socket.receive();
                String senderIP = packet.getAddress().getHostAddress();
                socket.send("", senderIP, SocketHelper.PING_PORT);
                if (!Server.group.contains(senderIP)) {
                    Server.writeToLog("Got ping from someone not in my list: " + senderIP);
                    Server.addToMemberList(senderIP);
                    disseminate();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class PingThread extends MyThread implements Runnable {
    private int i = 0;

    private String nextNeighbor() {
        i = (i + 1) % neighbors.length;
        int neighborIndex = (Server.group.indexOf(Server.ip) + neighbors[i]) % Server.group.size();
        neighborIndex = indexWrap(neighborIndex);
        return Server.group.get(neighborIndex);
    }

    private void pingNeighbor(String neighbor) throws IOException {
        Server.writeToLog("Sending ping to: " + neighbor);
        socket.send("", neighbor, SocketHelper.ACK_PORT);
    }

    private void waitForACK(String neighbor) throws IOException {
        try {
            socket.receive(FAIL_TIME);
            Server.writeToLog("Received ACK from: " + neighbor);
        } catch (SocketTimeoutException e) {
            Server.writeToLog(String.format("detected failure at: %s", neighbor));
            Server.removeFromMemberList(neighbor);
            disseminate();
        }
    }

    @Override
    public void run() {
        try {
            socket = new SocketHelper(SocketHelper.PING_PORT);
            while (true) {
                String neighbor = nextNeighbor();
                if (!neighbor.equals(Server.ip)) {
                    pingNeighbor(neighbor);
                    waitForACK(neighbor);
                }
                systemClockSleep(PROTOCOL_PERIOD);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class IntroducerThread extends MyThread implements Runnable {
    @Override
    public void run() {
        try {
            socket = new SocketHelper(SocketHelper.INTRODUCER_PORT);
            /*
             * The introducer thread is on an infinite loop to accept single IP addresses,
             * add them to its member list, and send out its updated member list to the entire group.
             */
            while (true) {
                DatagramPacket packet = socket.receive();
                String senderIP = SocketHelper.getStringFromPacket(packet);
                Server.addToMemberList(senderIP);
                disseminate();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class ConnectThread extends MyThread implements Runnable {
    @Override
    public void run() {
        try {
            socket = new SocketHelper(SocketHelper.CONNECT_PORT);
            while (true) {
                DatagramPacket packet = socket.receive();
                String newList = SocketHelper.getStringFromPacket(packet);
                Server.updateMemberList(newList.split(Server.IP_DELIMITER));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

/**
 * Contains a helper functions for UDP programming
 */
class SocketHelper {
    static final int CONNECT_PORT = 2020;
    static final int INTRODUCER_PORT = 2012;
    static final int PING_PORT = 2010;
    static final int ACK_PORT = 2011;
    Random random = new Random();

    private final int BUFFER_SIZE = 4096;
    private final int DEFAULT_TIMEOUT = 0; // disables timeout in DatagramSocket
    private byte[] buf = new byte[BUFFER_SIZE];
    private DatagramSocket socket;

    SocketHelper(int port) throws SocketException {
        socket = new DatagramSocket(port);
    }

    static String getStringFromPacket(DatagramPacket packet) {
        return new String(packet.getData(), packet.getOffset(), packet.getLength()).trim();
    }

    DatagramPacket receive() throws IOException {
        return receive(DEFAULT_TIMEOUT);
    }

    DatagramPacket receive(int timeout) throws IOException {
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        socket.setSoTimeout(timeout);
        socket.receive(packet);
        return packet;
    }

    void send(String data, String address, int port) throws IOException {
        byte[] buf = data.getBytes();
        InetAddress IP = InetAddress.getByName(address);
        DatagramPacket packet = new DatagramPacket(buf, buf.length, IP, port);
        socket.send(packet);
    }

    void close() {
        socket.close();
    }

}

