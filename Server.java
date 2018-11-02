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

    /* FOR DEBUGGING */
//    private static final String INTRODUCER_IP = "192.168.1.12";
//    private static final String INTRODUCER_IP = "10.195.57.170";
//    private static final String INTRODUCER_IP = "172.31.98.6";

    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss.SSS");
    public static final int SERVER_PORT = 2017;
    volatile static String ip;
    volatile static ArrayList<String> group = new ArrayList<>();
    private static ServerSocket serverSocket;
    static String machine;
    private static FileWriter log;

    private static ArrayList<String> removeDuplicatesAndSort(ArrayList<String> list) {
        Set<String> set = new HashSet<>(list);
        ArrayList<String> ret = new ArrayList<>(set);
        Collections.sort(ret);
        return ret;
    }

    static ArrayList<String> logGrep(String pattern, String logFileName) {
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

    public static String getStackTrace(final Throwable throwable) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw, true);
        throwable.printStackTrace(pw);
        return sw.getBuffer().toString();
    }

    static String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    private static String getCanonicalName() throws UnknownHostException {
        return InetAddress.getLocalHost().getCanonicalHostName();
    }

    private static String getIPAddress() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostAddress();
    }

    static String getLogFileName() {
        return String.format("%s.log", machine);
    }

    public static String getCurrentDateAsString() {
        return dateFormatter.format(new Date());
    }

    static void writeToLog(String msg) {
        try {
            String date = getCurrentDateAsString();
            String constructedLog = String.format("%s: %s\n", date, msg);
            System.out.print(constructedLog);
            log.write(constructedLog);
            log.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void addToMemberList(String ip) throws IOException {
        group.add(ip);
        group = removeDuplicatesAndSort(group);
        String date = getCurrentDateAsString();
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
        System.setOut(new PrintStream(new FileOutputStream(getLogFileName(), true)));
        writeToLog(String.format("Attempting to start server at: %s", ip));

        new FailureReplicaReceiveThread().start();
        new FailureReplicaCleanupThread().start();
        new FailureReplicaSendThread().start();

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
                DatagramPacket packet = socket.receive(FailureDetectionThread.PROTOCOL_PERIOD);

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
        serverSocket = new ServerSocket(SERVER_PORT);
        while (true) {
            writeToLog("Looking for new connection on server");
            ServerResponseThread srt = new ServerResponseThread(serverSocket.accept());
            srt.run();
        }
    }
}

class ServerResponseThread extends Thread {
    private Socket socket;

    public ServerResponseThread(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            DataInputStream reader = new DataInputStream(socket.getInputStream());
            DataOutputStream writer = new DataOutputStream(socket.getOutputStream());
            String cmd = reader.readUTF().trim();
            Server.writeToLog(String.format("Got command: %s", cmd));
            String[] cmds = cmd.split(" ");
            switch (cmds[0]) {
                /*
                 *  print:
                 *  This command prints out the current membership list on every available server
                 */
                case "print":
                    writer.writeBytes(String.format("My ID is: %s\n", Server.ip));
                    writer.writeBytes(String.format("%d members are in my group\n", Server.group.size()));
                    for (String server : Server.group)
                        writer.writeBytes(String.format("%s\n", server));
                    Server.writeToLog("Sent membership list to client");
                    break;
                /*
                 *  grep:
                 *  execute a grep command on the log file and send back the grep results
                 */
                case "grep":
                    if (cmds.length < 2) {
                        Server.writeToLog("grep command did not have the right arguments");
                        return;
                    }
                    // Extract pattern
                    int patternStart = cmd.indexOf(" ");
                    ArrayList<String> res = Server.logGrep(cmd.substring(patternStart + 1), Server.getLogFileName());
                    writer.writeBytes(String.join("\n", res));
                    Server.writeToLog("Sent grep output to client");
                    break;
                /*
                 *  quit:
                 *  exits if I was commanded to quit, else update my membership list
                 */
                case "quit":
                    if (cmds.length < 2) {
                        Server.writeToLog("quit command did not have the right arguments");
                        return;
                    }
                    String quitter = cmds[1];
                    if (Server.ip.equals(quitter)) {
                        System.exit(0);
                    } else {
                        Server.removeFromMemberList(quitter);
                    }
                    Server.writeToLog(String.format("%s naturally exited.", quitter));
                    break;
                /*
                log:
                sends back the log of a specific ip address
                 */
                case "log":
                    if (cmds.length < 2) {
                        Server.writeToLog("log command did not have the right arguments");
                        return;
                    }
                    if (Server.ip.equals(cmds[1]) || Server.machine.equals(cmds[1])) {
                        String fileContents = Server.readFile(Server.getLogFileName(), Charset.forName("UTF-8"));
                        writer.writeBytes(fileContents);
                    }
                    break;
                /*
                 ls:
                 checks if file exists on VM
                 */
                case "ls":
                    if (FileHandler.fileExists(cmds[1])) {
                        writer.writeBytes("+++++File found!");
                    } else {
                        writer.writeBytes("-----not found");
                    }
                    Server.writeToLog(String.format("Checked for %s.", cmds[1]));
                    break;
                /*
                put:
                receives a file from the client.
                 */
                case "put":
                    try {
                        File tmpFile = File.createTempFile(cmds[2], "");
                        tmpFile.deleteOnExit();
                        FileHandler.receiveFile(tmpFile, socket, true);
                        Server.writeToLog(String.format("Saved file to temp location: %s", tmpFile.getAbsolutePath()));

                        // Receive signal to save or delete
                        ReplicaReceiveThread receive = new ReplicaReceiveThread(tmpFile, writer, cmds[2]);
                        receive.start();
                        Server.writeToLog("Started replica receive thread");

                        // Master node needs to coordinate where files go
                        Server.writeToLog(String.format("Master node is: %s and my ip is %s", FileHandler.getMasterNodeIP(), Server.ip));
                        ReplicaMasterThread master = null;
                        if (Server.ip.equals(FileHandler.getMasterNodeIP())) {
                            Server.writeToLog("I am the master node!");
                            master = new ReplicaMasterThread(cmds[2]);
                            master.start();
                            Server.writeToLog("Started replica master thread");
                        }

                        try {
                            if (master != null) {
                                master.join();
                                Server.writeToLog("Master thread successfully joined");
                            }
                            receive.join();
                            Server.writeToLog("Replica threads finished");
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                    } catch (IOException e) {
                        Server.writeToLog("File did not exist on the local host so they closed socket");
                    }
                    break;
                /*
                get:
                sends a file to the client
                 */
                case "get":
                    try {
                        int versions = FileHandler.numVersions(new File(FileHandler.getFilePath(cmds[1])));
                        FileHandler.sendFile(cmds[1], socket, versions);
                        Server.writeToLog(String.format("Sent file: %s", cmds[1]));
                    } catch (IOException e) {
                        // If we could not find the file on our sdfs, then simply close socket to signal DNE
                        socket.close();
                        Server.writeToLog(String.format("IOException: %s", e.getMessage()));
                    }
                    break;
                /*
                get-versions:
                get all versions of a file
                */
                case "get-versions":
                    int numVersions = FileHandler.numVersions(new File(FileHandler.getFilePath(cmds[1])));
                    Server.writeToLog(String.format("get-versions numVersions: %d", numVersions));
                    int numVersionsRequested = Integer.parseInt(cmds[2]);
                    Server.writeToLog(String.format("get-versions numVersionsRequested: %d", numVersionsRequested));
                    // Concatenate all versions into a tmp file
                    File tmpFile = File.createTempFile(cmds[1], "");
                    tmpFile.deleteOnExit();
                    Server.writeToLog(String.format("get-versions tmpFile: %s", tmpFile.getAbsolutePath()));
                    if (numVersions - numVersionsRequested > -1) {
                        Server.writeToLog("get-versions: Concatenating versions to a temp file");
                        for (int i = numVersions - numVersionsRequested; i < numVersions; i++) {
                            int version = i + 1; // i + 1 because versions are 1-indexed
                            String filePath = FileHandler.getFilePath(cmds[1]);
                            File versionFile = FileHandler.getVersionContent(new File(filePath), version, false);
                            FileHandler.appendFileToFile(tmpFile, versionFile);
                            Server.writeToLog(String.format("get-versions appended version: %d", version));
                        }
                        Server.writeToLog(String.format("get-versions Sending concatenated versions from: %s", tmpFile.getAbsolutePath()));
                        FileHandler.sendFile(tmpFile, socket, -1);
                    } else {
                        Server.writeToLog("get-versions: Client requested too many versions");
                        socket.close();
                    }
                    break;
                /*
                get:
                sends a file to the client
                 */
                case "delete":
                    FileHandler.deleteFile(cmds[1]);
                    writer.writeBytes(String.format("Deleted %s", cmds[1]));
                    break;
                default:
                    Server.writeToLog(String.format("Received invalid command: %s", cmds[0]));
                    break;
            }
            socket.close();
        } catch (IOException e) {
            Server.writeToLog(Server.getStackTrace(e));
        }

    }

}

class FailureReplicaSendThread extends Thread {
    @Override
    public void run() {
        while (true) {
            try {
                Server.writeToLog("Re-replicating files on my sdfs");
                for (File file : FileHandler.getFiles()) {
                    Server.writeToLog(String.format("Re-replicating: %s", file.getName()));
                    for (int i = 0; i < Server.group.size(); i++) {
                        if (FileHandler.isReplicaNode(file.getName(), i)) {
                            Server.writeToLog(String.format("Re-replicating %s on %s", file.getName(), Server.group.get(i)));
                            Socket socket;
                            socket = new Socket(Server.group.get(i), FileHandler.FAILURE_REPLICA_PORT);
                            Server.writeToLog(String.format("Established connection to %s", Server.group.get(i)));
                            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                            out.writeUTF(file.getName());
                            Server.writeToLog(String.format("Sent %s to %s", file.getName(), Server.group.get(i)));
                            FileHandler.sendFile(file, socket, -1); // Send entire file
                            Server.writeToLog("Sent re-replication file");
                        }
                    }
                }
                Thread.sleep(1500);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

class FailureReplicaCleanupThread extends Thread {

    public FailureReplicaCleanupThread() {
        Server.writeToLog("Instantiated FailureReplicaCleanupThread");
    }

    @Override
    public void run() {
        while(true) {
            try {
                int myIndex = Server.group.indexOf(Server.ip);
                if (myIndex != -1) {
                    for (File file : FileHandler.getFiles()) {
                        if (!FileHandler.isReplicaNode(file.getName(), myIndex)) {
                            Server.writeToLog(String.format("Deleting-file: %s", file.getName()));
                            Server.writeToLog(String.format("Deleting-hashcode: %s", file.getName().hashCode()));
                            Server.writeToLog(String.format("Deleting-file-node: %d", FileHandler.getNodeFromFile(file.getName())));
                            Server.writeToLog(String.format("Deleting-group: %s", Server.group.toString()));
                            Server.writeToLog(String.format("Deleting-myIndex: %d", myIndex));
                            Server.writeToLog(String.format("Deleting-Server.ip: %s", Server.ip));
                            file.delete();
                        }
                    }
                }
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Server.writeToLog(e.getMessage());
            }
        }
    }
}

class FailureReplicaReceiveThread extends Thread {
    ServerSocket serverSocket;

    public FailureReplicaReceiveThread() {
        try {
            serverSocket = new ServerSocket(FileHandler.FAILURE_REPLICA_PORT);
            Server.writeToLog("Instantiated failure replica receive server socket");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                Server.writeToLog("Got connection for re-replication");
                DataInputStream in = new DataInputStream(socket.getInputStream());
                String filename = in.readUTF().trim();
                if (FileHandler.isReplicaNode(filename, Server.group.indexOf(Server.ip))) {
                    Server.writeToLog(String.format("File to be re-replicated on me: %s", filename));
                    if (!FileHandler.fileExists(filename)) {
                        FileHandler.receiveFile(filename, socket, false);
                        Server.writeToLog(String.format("Saved re-replication file: %s", filename));
                    } else {
                        Server.writeToLog(String.format("Re-replication file already exists: %s", filename));
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}

class ReplicaReceiveThread extends Thread {
    File file;
    String filename;
    DataOutputStream writer;

    public ReplicaReceiveThread(File file, DataOutputStream writer, String filename) {
        this.file = file;
        this.writer = writer;
        this.filename = filename;
    }

    @Override
    public void run() {
        try {
            if (FileHandler.receiveReplicaSignal()) {
                // Signaled to save
                FileHandler.appendFileToFile(file, new File(FileHandler.getFilePath(filename)));
                writer.writeBytes("File saved ACK");
                Server.writeToLog("Received replica signal to save");
            } else {
                // Signaled to delete
                file.delete();
                writer.writeBytes("File deleted ACK");
                Server.writeToLog("Received replica signal to delete");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class ReplicaMasterThread extends Thread {
    String filename;

    public ReplicaMasterThread(String filename) {
        this.filename = filename;
    }

    @Override
    public void run() {
        try {
            // Give the signal to either save or delete the temporary file
            for (int i = 0; i < Server.group.size(); i++) {
                Server.writeToLog(String.format("Sending replica signal to %s", Server.group.get(i)));
                FileHandler.sendReplicaSignal(Server.group.get(i), (FileHandler.isReplicaNode(this.filename, i)));
                Server.writeToLog(String.format("Sent replica signal to %s", Server.group.get(i)));
            }
        } catch (IOException e) {
            Server.writeToLog(e.getMessage());
        }
    }
}

class FailureDetectionThread extends Thread {
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

class AckThread extends FailureDetectionThread implements Runnable {
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

class PingThread extends FailureDetectionThread implements Runnable {
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

class IntroducerThread extends FailureDetectionThread implements Runnable {
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

class ConnectThread extends FailureDetectionThread implements Runnable {
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

