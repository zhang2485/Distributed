import java.io.*;
import java.net.*;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.nio.file.Files;

class FileHandler {
    static final String DIRECTORY = "user.dir";
    static final int BUFFER_SIZE  = 2048;
    static final int MASTERPORT = 1234;
    static File[] getFiles() {
        File directory = new File(System.getProperty(DIRECTORY));
        return directory.listFiles();
    }

    static void printFiles() {
        for (File f : getFiles())
            System.out.println(f.getName());
    }

    static boolean fileExists(String filename) {
        for (File f: getFiles()) {
            if (f.getName().equals(filename)) return true;
        }
        return false;
    }

    static String getFilePath(String filename) {
        return String.format("%s/%s", System.getProperty(DIRECTORY), filename);
    }

    static void sendFile(String filename, Socket socket) throws IOException {
        // Instantiate streams
        File file = new File(getFilePath(filename));
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        if(!file.exists()) {
        	String dne = "DNE";
        	byte[] buff = dne.getBytes();
        	out.write(buff, 0, buff.length);
        	out.close();
        	return;
        }
        FileInputStream in = new FileInputStream(file);
        long numBytes = file.length();
        out.writeLong(numBytes);

        // Send the file
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((count = in.read(buffer)) > 0) {
            out.write(buffer, 0, count);
        }
        in.close();
    }

    static void receiveFile(String filename, Socket socket) throws IOException {
        // Instantiate streams
        FileOutputStream out = new FileOutputStream(getFilePath(filename));
        DataInputStream in = new DataInputStream(socket.getInputStream());

        long numBytes = in.readLong();

        // Receive and write to file
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((count = in.read(buffer)) > 0) {
            out.write(buffer, 0, count);
            numBytes -= count;
            if (numBytes == 0)
                break;
        }
        out.close();
    }
    
    static void emptyPipe(String filename, Socket socket) throws IOException {
        DataInputStream in = new DataInputStream(socket.getInputStream());
        long numBytes = in.readLong();
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((count = in.read(buffer)) > 0) {
            numBytes -= count;
            if (numBytes == 0)
                break;
        }
    }
    static void potentialFile(String filename, Socket socket) throws IOException {
    	byte[] buf = new byte[2048];
    	DatagramSocket sock = new DatagramSocket(MASTERPORT, InetAddress.getByName(Server.ip));
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        sock.setSoTimeout(0);
        System.out.println("75");
        sock.receive(packet);
        System.out.println("76");
        sock.close();
        String selected = new String(buf);
        System.out.println(selected);
        Server.fileList.put(filename, selected);
        if(selected.contains(Server.ip)) {
        	receiveFile(filename, socket);
        }
        else {
        	// Empty the pipe
        	emptyPipe(filename, socket);
        }
    }
}



public class Server {
    static final String IP_DELIMITER = " ";
//    private static final String INTRODUCER_IP = "192.168.1.14";
    private static final String INTRODUCER_IP = "172.22.154.22";
    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss.SSS");
    private static final int SERVER_PORT = 2017;
    volatile static String ip;
    volatile static ArrayList<String> group = new ArrayList<>();
    private volatile static HashMap<String, String> connectTimes = new HashMap<>();
    volatile static HashMap<String, String> fileList = new HashMap<>();
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

    static void writeToLog(String msg) throws IOException {
        String date = getCurrentDateAsString();
        String constructedLog = String.format("%s: %s\n", date, msg);
        //System.out.print(constructedLog);
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
        // This will alert other nodes of need for replication (213) Master Node is last member of List
        if(Server.group.get(Server.group.size() - 1).equals(Server.ip)) {
        	for(String k: Server.fileList.keySet()) {
        		// Look for files lost with loss of group member, list of members that contain the file
        		String list = Server.fileList.get(k);
        		if(list.contains(ip)) {
        			List<String> temp = new ArrayList<String>(Server.group);
        			int choice = (int) (Math.random() * (temp.size() - 1));
        			while(list.contains(Server.group.get(choice))) {
        				choice = (int) (Math.random() * (temp.size() - 1));
        			}
        			String [] str = list.split(",");
        			for(int i = 0; i < str.length; i++) {
        				if(str[i].equals(ip)) {
        					str[i] = Server.group.get(choice);
        				}
        			}
        			String newFileList = String.join("," , str);
        			for(String s: temp) {
        				Socket sendList = new Socket(s, 2048);
        				DataOutputStream out = new DataOutputStream(sendList.getOutputStream());
        				out.writeBytes(k + ":" + newFileList);
        				System.out.println(k + ":" + newFileList);
        				sendList.close();
        			}
        		}
        	}
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
            new fileThread().start();
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
        serverSocket = new ServerSocket(SERVER_PORT);
        while (true) {
            writeToLog("Looking for new connection on server");
            ServerResponseThread srt = new ServerResponseThread(serverSocket.accept());
            srt.run();
            srt.join(); // Handle connections sequentially to achieve total ordering!
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
                        writer.writeBytes("File found!");
                    } else {
                        writer.writeBytes("File not found...");
                    }
                    Server.writeToLog(String.format("Checked for %s.", cmds[1]));
                    break;
                /*
                put:
                receives a file from the client
                 */
                case "put":
                    Server.writeToLog(String.format("Received put command: '%s'", cmd));
                    // The Master node will be index 1 in the membership list
                    // Simple in design effective in accomplishing the task
                    System.out.println(Server.group);
                    System.out.println(Server.ip);
                    if(Server.group.get(Server.group.size() - 1).trim().equals(Server.ip.trim())) {
                    	List<String> temp = new ArrayList<String>(Server.group);
                    	Collections.shuffle(temp);
                    	temp = temp.subList(0, 4);
                    	String chosen = String.join(",", temp);
                        byte[] buf = chosen.getBytes();
                        DatagramSocket sock = new DatagramSocket();
                        for(int i = 0; i < Server.group.size(); i++) {
	                        InetAddress IP = InetAddress.getByName(Server.group.get(i));
	                        if(Server.group.get(i).equals(Server.ip.trim())) {
	                        	continue;
	                        }
	                        DatagramPacket packet = new DatagramPacket(buf, buf.length, IP, 1234);
	                        sock.send(packet);
	                        System.out.println("389");
	                        Thread.sleep(600);
                        }
                        sock.close();
                        if(chosen.contains(Server.ip.trim())) {
                        	FileHandler.receiveFile(cmds[2], socket);
                        }
                        else {
                        	FileHandler.emptyPipe(cmds[2], socket);
                        }
                    }
                    else {
                    	FileHandler.potentialFile(cmds[2], socket);
                    }
                    writer.writeBytes("RECIEVED");
                    break;
                /*
                get:
                sends a file to the client
                 */
                case "get":
                    Server.writeToLog(String.format("Received get command: '%s'", cmd));
                    FileHandler.sendFile(cmds[1], socket);
                    Server.writeToLog(String.format("Sent file: %s", cmds[1]));
                    break;
                default:
                    Server.writeToLog(String.format("Received invalid command: %s", cmds[0]));
                    break;
            }
            socket.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
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
    public void replicateFiles(String failed) {
    	for(int j = 0; j < Server.fileList.size(); j++) {
    		
    	}
    }
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
 * This thread is used for recieving new file membership lists, and sending Files over if applicable
 * (615)
 */
class fileThread extends MyThread implements Runnable {
	
    @Override
    public void run() {
    	
        try {
            @SuppressWarnings("resource")
			ServerSocket fileSocket = new ServerSocket(2048);
            @SuppressWarnings("resource")
			ServerSocket receiveSocket = new ServerSocket(2049);
            while (true) {
            	Socket accepted = fileSocket.accept();
            	BufferedReader reader = new BufferedReader(new InputStreamReader(accepted.getInputStream()));
            	String fileInfo = reader.readLine();
            	String [] membership = fileInfo.split(":");
            	String file = membership[0].trim();
            	String newMembers = membership[1].trim();
            	File f = new File(file);
            	if(f.exists()) {
            		System.out.println("I have the file, I will now try to replicate.");
            		String origList = Server.fileList.get(file);
            		String [] newMem = newMembers.split(",");
            		for(int i = 0; i < newMem.length; i++) {
            			if(!origList.contains(newMem[i].trim())) {
            				// We'll send the file to the new participant in the membership list
            				Socket sock = new Socket(newMem[i], 2049);
            				FileHandler.sendFile(file, sock);
            				sock.close();
            			}
            		}
            	}
            	if(!f.exists() && newMembers.contains(Server.ip)) {
            		// new member list
            		Socket fileReceiver = receiveSocket.accept();
            		FileHandler.receiveFile(file, fileReceiver);
            		fileReceiver.close();
            	}
            	Server.fileList.put(file, newMembers);
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
    static final int INTRODUCER_PORT = 2013;
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

