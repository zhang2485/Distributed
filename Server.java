import java.io.*;
import java.net.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.nio.file.Files;

import static java.lang.Math.abs;

class FileHandler {
    static final String USER_DIR = "user.dir";
    static final String SDFS_DIR = "sdfs";
    static final int BUFFER_SIZE  = 1000000;
    static final int REPLICA_PORT = 2118;
    static final int FAILURE_REPLICA_PORT = 5001;
    static final String DELIMITER = "\n--NEW FILE--\n";

    static File[] getFiles() throws IOException {
        Files.createDirectories(Paths.get(getDirectoryPath()));
        File directory = new File(getDirectoryPath());
        return directory.listFiles();
    }

    static File[] listVersions(String filename) {
        File file = new File(getFilePath(filename));
        if (file.isDirectory()) {
            return file.listFiles();
        }
        return new File[]{file};
    }


    static String getMasterNodeIP() {
        return Server.group.get(0);
    }

    static int getNodeFromFile(String filename) {
        return abs((filename.hashCode() % 10) % Server.group.size());
    }

    static void printFiles() throws IOException {
        for (File f : getFiles())
            System.out.println(f.getName());
    }

    static boolean isReplicaNode(String filename, int idx) {
        int numReplicas = 4;
        int lower = getNodeFromFile(filename);
        int upper = (lower + numReplicas) % Server.group.size();
        if (lower < upper) {
            return idx >= lower && idx < upper;
        }
        return idx < upper || idx >= lower;
    }

    static boolean fileExists(String filename) {
        return Files.exists(Paths.get(getFilePath(filename)));
    }

    static String getDirectoryPath() {
        return String.format("%s/%s", System.getProperty(USER_DIR), SDFS_DIR);
    }

    static String getFilePath(String filename) {
        return String.format("%s/%s", getDirectoryPath(), filename);
    }

    static Path getNewFilePath(String filename) throws IOException {
        if (!fileExists(filename))
            Files.createDirectories(Paths.get(getFilePath(filename)));
        String newFilePath = String.format("%s/%s", getFilePath(filename), fileNameSafeString(Server.getCurrentDateAsString()));
        while (Files.exists(Paths.get(newFilePath)));
            newFilePath = String.format("%s/%s", getFilePath(filename), fileNameSafeString(Server.getCurrentDateAsString()));
        return Files.createFile(Paths.get(newFilePath));
    }

    static String fileNameSafeString(String filename) {
        return filename.replaceAll("[^a-zA-Z0-9\\.\\-]", "_");
    }

    static void appendFileToFile(File src, File dst) throws IOException {
        FileInputStream in =  new FileInputStream(src);
        FileOutputStream out = new FileOutputStream(dst, true);
        byte[] buffer = new byte[BUFFER_SIZE];
        int count;
        while ((count = in.read(buffer)) > 0) {
            out.write(buffer, 0, count);
        }
    }

    static void deleteFile(String filename) throws IOException {
        for (File file : listVersions(filename)) {
            file.delete();
        }
        new File(getFilePath(filename)).delete();
    }

    static void sendReplicaSignal(String ip, boolean signal) throws IOException {
        boolean scanning = true;
        Socket socket;
        while(scanning) try {
            socket = new Socket(ip, REPLICA_PORT);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeBoolean(signal);
            scanning = false;
        } catch (ConnectException e) {
            continue;
        }
    }

    static boolean receiveReplicaSignal() throws IOException {
        ServerSocket serverSocket = new ServerSocket(REPLICA_PORT);
        Socket socket = serverSocket.accept();
        DataInputStream in = new DataInputStream(socket.getInputStream());
        boolean signal = in.readBoolean();
        serverSocket.close();
        return signal;
    }

    static int numVersions(String filename) throws IOException {
        File file = new File(getFilePath(filename));
        if (file.isDirectory())
            return file.list().length;
        return 1;
    }

    static File getVersionContent(String filename, int version) throws IOException, IndexOutOfBoundsException {
        File[] versions = listVersions(filename);
        if (versions.length == 0) {
            deleteFile(filename);
            throw new IOException("There are no versions of this file");
        }

        Arrays.sort(versions, new Comparator<File>(){
            public int compare(File f1, File f2) {
                return Long.valueOf(f1.lastModified()).compareTo(f2.lastModified());
            }
        });
        return versions[version];
    }

    static void sendFile(File file, Socket socket, int version) throws IOException {
        sendFile(file.getName(), socket, version);
    }

    static void sendFile(String filename, Socket socket, int version) throws IOException {
        // Instantiate streams
        File fileVersion = getVersionContent(filename, version);
        FileInputStream in = new FileInputStream(fileVersion);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        long numBytes = fileVersion.length();
        // Handle empty files by throwing an exception
        if (numBytes <= 0) {
            fileVersion.delete();
            throw new IOException("Tried to send empty file");
        }
        out.writeLong(numBytes);

        // Send the file
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((count = in.read(buffer)) > 0) {
            out.write(buffer, 0, count);
        }
    }

    static void sendFile(File file, Socket socket) throws IOException {
        // Instantiate streams
        FileInputStream in = new FileInputStream(file);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        long numBytes = file.length();
        // Handle empty files by throwing an exception
        if (numBytes <= 0) {
            file.delete();
            throw new IOException("Tried to send empty file");
        }
        out.writeLong(numBytes);

        // Send the file
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((count = in.read(buffer)) > 0) {
            out.write(buffer, 0, count);
        }

    }

    static void receiveFile(String filename, Socket socket) throws IOException {
        File file = getNewFilePath(filename).toFile();
        FileOutputStream out = new FileOutputStream(file);
        DataInputStream in = new DataInputStream(socket.getInputStream());

        long numBytes = in.readLong();

        // Receive and write to file
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((count = in.read(buffer)) > 0) {
            out.write(buffer, 0, count);
            if ((numBytes -= count) == 0)
                break;
        }
    }

}

public class Server {
    static final String IP_DELIMITER = " ";
    private static final String INTRODUCER_IP = "172.22.156.255";

    /* FOR DEBUGGING */
//    private static final String INTRODUCER_IP = "192.168.1.12";
//    private static final String INTRODUCER_IP = "10.195.57.170";
//    private static final String INTRODUCER_IP = "172.31.98.6";

    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss.SSS");
    public static final int SERVER_PORT = 2217;
    volatile static String ip;
    volatile static ArrayList<String> group = new ArrayList<>();
    private static ServerSocket serverSocket;
    static String machine;
    private static FileWriter log;
    private static final boolean DEBUG = false;

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
        } catch (Exception e) {
            Server.writeToLog(e);
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

    static void writeToLog(final Throwable throwable) {
        writeToLog(getStackTrace(throwable));
    }

    static void writeToLog(String msg) {
        try {
            if (DEBUG) {
                String date = getCurrentDateAsString();
                String constructedLog = String.format("%s: %s\n", date, msg);
                System.out.print(constructedLog);
                log.write(constructedLog);
                log.flush();
            }
        } catch (IOException e) {
            Server.writeToLog(e);
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
        System.out.println("Looking for new connection on server");
        while (true) {
            writeToLog("Looking for new connection on server");
            ServerResponseThread srt = new ServerResponseThread(serverSocket.accept());
            srt.run();
        }
    }
}

/*
 * Worker thread processes the storm instructions
 */
class boltThread extends Thread {
	int port = 6425;
	String inst;
	String [] bolts;
	DataOutputStream writer;
	public boltThread(String inst, DataOutputStream writer) {
		System.out.println("BOLT THREAD");
		inst = inst.replace("crane", "");
		inst = inst.trim();
		this.inst = inst.split("<>")[0];
		bolts = this.inst.split(",");
		this.writer = writer;
	}
	
	public ArrayList<String> filter(String regex, ArrayList<String> lines){
		for(int i = 0; i < lines.size(); i++) {
			String line = lines.get(i);
			if(!line.contains(regex)) {
				lines.remove(i);
				i = i - 1;
			}
		}
		return lines;
	}
	
	public ArrayList<String> transform(String regex, String replace, ArrayList<String> lines){
		for(int i = 0; i < lines.size(); i++) {
			String line = lines.get(i);
			line = line.replace(regex,  replace);
			lines.set(i, line);
		}
		return lines;
	}
	
	public ArrayList<String> transform(String function, ArrayList<String> lines){
		if(function.contains("upper")) {
			for(int i = 0; i < lines.size(); i++) {
				String line = lines.get(i);
				line = line.toUpperCase();
				lines.set(i, line);
			}
		}
		else if(function.contains("lower")) {
			for(int i = 0; i < lines.size(); i++) {
				String line = lines.get(i);
				line = line.toLowerCase();
				lines.set(i, line);
			}
		}
		return lines;
	}
	
	public void run() {
		try {
			ServerSocket ss = new ServerSocket(port);
			Socket s = ss.accept();
			/*
			 * Obtain list of tuples to process
			 */
			ArrayList<String> lines = new ArrayList<>();
			BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			while(!br.ready()) {
				// Wait for the file to perform bolts on
			}
			String line = null;
			while((line = br.readLine())!=null) {
				System.out.println(line);
				lines.add(line + '\n');
			}
			/*
			 * Every worker will perform all of the bolts in chained order
			 * declared by user
			 */
			for(int i = 0; i < bolts.length; i++) {
				String instruction = bolts[i];
				System.out.println(instruction);
				String [] parts = instruction.split(" ");
				if(parts[0].contains("filter")) {
					String regex = parts[1];
					lines = filter(regex, lines);
				}
				else if(parts[0].contains("transform")) {
					if(parts.length==2) {
						//apply a string function
						lines = transform(parts[1], lines);
					}
					else {
						//transform from one substring to another
						lines = transform(parts[1], parts[2], lines);
					}
				}
			}
			/*
			 * Transform arrayList back into string after all bolts have finished 
			 */
			StringBuilder sb = new StringBuilder();
			for(String l: lines) {
				sb.append(l);
			}
			
			/*
			 * Return the finished string to the user
			 */
			writer.write(sb.toString().getBytes());
			ss.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

/*
 * Spout thread splits the static database 
 * file into even parts and evenly distributes the work
 * among the worker nodes.
 */
class spoutThread extends Thread{
	int port = 6425;
	String inst;
	String db;
	/**
	 *  Converts the chunks into strings before sending them 
	 *  to worker threads for processing
	 * @param lines
	 * @return string
	 */
	public String convertToString(List<String> lines) {
		StringBuilder sb = new StringBuilder();
		for(String line: lines) {
			sb.append(line + '\n');
		}
		return new String(sb);
	}
	/*
	 * Spout thread opens up database from the instruction set
	 */
	public spoutThread(String inst) {
		System.out.println("SPOUT THREAD");
		this.inst = inst;
		System.out.println(inst);
		this.db = inst.split("<>")[1];
	}
	
	public void run() {
		try {
			/*
			 * partition database into even blocks, evenly split
			 * between active worker nodes that are not spouts
			 */
			List<String> lines = Files.readAllLines(Paths.get(db), StandardCharsets.UTF_8);
			String names = lines.remove(0);
			int partitions = Server.group.size() - 1;
			int partition = lines.size()/partitions;
			List<List<String>> blocks = new LinkedList<List<String>>();
			ArrayList<String> cpy = new ArrayList<>(Server.group);
			// Remove spout ip from copy of group (only keep worker nodes)
			cpy.remove(Server.ip.trim());
			for (int i = 0; i < lines.size(); i += partition) {
			    blocks.add(lines.subList(i, Math.min(i + partition, lines.size())));
			}
			/*
			 * Send each worker node a copy of evenly divided stream
			 * from static database
			 */
			for(int i = 0; i < cpy.size(); i++) {
				Socket spout = new Socket(cpy.get(i), 6425);
				DataOutputStream writer = new DataOutputStream(spout.getOutputStream());
				String stream = convertToString(blocks.get(i));
				writer.write(stream.getBytes());
				spout.close();
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
        	System.out.println("GET COMMAND");
            DataInputStream reader = new DataInputStream(socket.getInputStream());
            DataOutputStream writer = new DataOutputStream(socket.getOutputStream());
            String cmd = reader.readUTF().trim();
            Server.writeToLog(String.format("Got command: %s", cmd));
            cmd = cmd.trim();
            String[] cmds = cmd.split(" ");
            switch (cmds[0]) {
            		/*
            		 * crane:
            		 * initializes spoutThread or boltThread to perform crane operations on 
            		 * a database, returns row results to the client
            		 */
            		case "crane":
            			//Remove crane from commands list
            			cmds[0] = "";
            			cmd = String.join(" ", cmds);
            			cmd = cmd.trim();
            			if(Server.ip.equals(Server.group.get(1))) {
            				//Spout thread for assigning work to workers
            				spoutThread sT = new spoutThread(cmd);
            				sT.start();
            				sT.join();
            			}
            			else {
            				//Bolt thread to initiate workers.
            				boltThread bT = new boltThread(cmd, writer);
            				bT.start();
            				bT.join();
            			}
            			break;
           
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
                receives a file from the client. Saves the file to a tmp file and then saves the file into the sdfs
                if the master node sends a signal to save, otherwise we delete the tmp file and move on.
                 */
                case "put":
                    // Receive signal to save or not
                    ReplicaReceiveThread receive = new ReplicaReceiveThread(socket, writer, cmds[2]);
                    receive.start();
                    Server.writeToLog("Started replica receive thread");

                    // Master node needs to coordinate where files go by sending the save/delete signals
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
                        Server.writeToLog(e);
                    }
                    break;
                /*
                get:
                sends a file to the client
                 */
                case "get":
                    try {
                        if (FileHandler.fileExists(cmds[1])) {
                            Server.writeToLog("get: File exists and i'm signalling that I have it");
                            writer.writeBoolean(true);
                            Server.writeToLog("get: signaled yes");
                            int versions = FileHandler.numVersions(cmds[1]);
                            Server.writeToLog("get: sending file");
                            FileHandler.sendFile(cmds[1], socket, versions - 1);
                            Server.writeToLog(String.format("get: sent file %s", cmds[1]));
                        }
                    } catch (FileNotFoundException e) {
                        // If we could not find the file on our sdfs, then simply close socket to signal DNE
                        Server.writeToLog(String.format("IOException: %s", e.getMessage()));
                    }
                    break;
                /*
                get-versions:
                get all versions of a file. Writes all versions to a temporary file and then sends that temp file
                back to the client.
                */
                case "get-versions":
                    try {
                        if (FileHandler.fileExists(cmds[1])) {
                            Server.writeToLog("get-versions: File exists and i'm signalling that I have it");
                            writer.writeBoolean(true);
                            Server.writeToLog("get-versions: signaled yes");
                            int numVersions = FileHandler.numVersions(cmds[1]);
                            Server.writeToLog(String.format("get-versions numVersions: %d", numVersions));
                            int numVersionsRequested = Integer.parseInt(cmds[2]);
                            Server.writeToLog(String.format("get-versions numVersionsRequested: %d", numVersionsRequested));
                            // Concatenate all versions into a tmp file
                            Path tmpFile = Files.createTempFile(null, null);
                            Server.writeToLog(String.format("get-versions tmpFile: %s", tmpFile.getFileName()));
                            if (numVersions - numVersionsRequested > -1) {
                                Server.writeToLog("get-versions: Concatenating versions to a temp file");
                                FileOutputStream out = new FileOutputStream(tmpFile.toFile(), true);
                                for (int i = numVersions - numVersionsRequested; i < numVersions; i++) {
                                    File versionFile = FileHandler.getVersionContent(cmds[1], i);
                                    FileHandler.appendFileToFile(versionFile, tmpFile.toFile());
                                    out.write(FileHandler.DELIMITER.getBytes());
                                    Server.writeToLog(String.format("get-versions appended version: %d", i));
                                }
                                Server.writeToLog(String.format("get-versions Sending concatenated versions: %s", tmpFile.getFileName()));
                                FileHandler.sendFile(tmpFile.toFile(), socket);
                            } else {
                                Server.writeToLog("get-versions: Client requested too many versions");
                            }
                        }
                    } catch (FileNotFoundException e) {
                        Server.writeToLog(e);
                    }
                    break;
                /*
                get:
                sends a file to the client
                 */
                case "delete":
                    if (FileHandler.fileExists(cmds[1])) {
                        FileHandler.deleteFile(cmds[1]);
                        writer.writeBytes(String.format("Deleted %s", cmds[1]));
                    } else {
                        writer.writeBytes(String.format("File did not exist %s", cmds[1]));
                    }
                    break;
                default:
                    Server.writeToLog(String.format("Received invalid command: %s", cmds[0]));
                    break;
            }
            socket.close();
        } catch (IOException | InterruptedException e) {
            Server.writeToLog(e);
        }

    }

}

class FailureReplicaSendThread extends FailureDetectionThread {
    final int TIMEOUT = 2000;
    @Override
    public void run() {
        while (true) {
            try {
                Server.writeToLog("Re-replicating files on my sdfs");
                for (File file : FileHandler.getFiles()) {
                    Server.writeToLog(String.format("Re-replicating: %s", file.getName()));
                    for (int i = 0; i < Server.group.size(); i++) {
                        if (FileHandler.isReplicaNode(file.getName(), i)) {
                            Server.writeToLog(String.format("Re-replicating %s to %s", file.getName(), Server.group.get(i)));
                            Socket socket = new Socket(Server.group.get(i), FileHandler.FAILURE_REPLICA_PORT);
                            socket.setSoTimeout(TIMEOUT);
                            Server.writeToLog(String.format("Established connection to %s", Server.group.get(i)));
                            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                            DataInputStream in = new DataInputStream(socket.getInputStream());
                            out.writeUTF(file.getName());
                            boolean sendOrNot = in.readBoolean();
                            if (sendOrNot) {
                                Server.writeToLog(String.format("Sending %s to %s", file.getName(), Server.group.get(i)));
                                int versions = FileHandler.numVersions(file.getName());
                                FileHandler.sendFile(file, socket, versions - 1); // Send entire file
                                Server.writeToLog("Sent re-replication file");
                            } else {
                                Server.writeToLog("Did not send re-replication file");
                            }
                        }
                    }
                }
                systemClockSleep(1500);
            } catch (IOException e) {
                Server.writeToLog(e);
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
                            FileHandler.deleteFile(file.getName());
                        }
                    }
                }
                Thread.sleep(500);
            } catch (IOException | InterruptedException e) {
                Server.writeToLog(e.getMessage());
            }
        }
    }
}

class FailureReplicaReceiveThread extends Thread {
    ServerSocket serverSocket;
    final int TIMEOUT = 2000;

    public FailureReplicaReceiveThread() {
        try {
            serverSocket = new ServerSocket(FileHandler.FAILURE_REPLICA_PORT);
            Server.writeToLog("Instantiated failure replica receive server socket");
        } catch (IOException e) {
            Server.writeToLog(e);
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                socket.setSoTimeout(TIMEOUT);
                Server.writeToLog(String.format("Got connection for re-replication from %s", socket.getRemoteSocketAddress().toString()));
                DataInputStream in = new DataInputStream(socket.getInputStream());
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                String filename = in.readUTF().trim();
                Server.writeToLog(String.format("File to be re-replicated on me: %s", filename));
                if (!FileHandler.fileExists(filename)) {
                    Server.writeToLog("File does not exist for me, accepting re-replication file");
                    out.writeBoolean(true);
                    FileHandler.receiveFile(filename, socket);
                    Server.writeToLog(String.format("Saved re-replication file: %s", filename));
                } else {
                    out.writeBoolean(false);
                    Server.writeToLog(String.format("Re-replication file already exists: %s", filename));
                }
            } catch (IOException e) {
                Server.writeToLog(e);
            }
        }
    }

}

class ReplicaReceiveThread extends Thread {
    Socket socket;
    String filename;
    DataOutputStream writer;

    public ReplicaReceiveThread(Socket socket, DataOutputStream writer, String filename) {
        this.socket = socket;
        this.writer = writer;
        this.filename = filename;
    }

    @Override
    public void run() {
        try {
            if (FileHandler.receiveReplicaSignal()) {
                // Signaled to save
                Server.writeToLog("Received replica signal to save");
                FileHandler.receiveFile(filename, socket);
                Server.writeToLog(String.format("Saved file: %s", filename));
                writer.writeBytes("File saved ACK");
            } else {
                Server.writeToLog("Received replica signal to not save");
            }
        } catch (IOException e) {
            Server.writeToLog(e);
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
                boolean signal = FileHandler.isReplicaNode(filename, i);
                Server.writeToLog(String.format("Master-thread-filename: %s", filename));
                Server.writeToLog(String.format("Master-thread-signal: %b", signal));
                Server.writeToLog(String.format("Master-thread-hashcode: %s", filename.hashCode()));
                Server.writeToLog(String.format("Master-thread-file-node: %d", FileHandler.getNodeFromFile(filename)));
                Server.writeToLog(String.format("Master-thread-group: %s", Server.group.toString()));
                Server.writeToLog(String.format("Master-thread-i: %d", i));
                Server.writeToLog(String.format("Master-thread-Server.ip: %s", Server.group.get(i)));
                FileHandler.sendReplicaSignal(Server.group.get(i), signal);
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
            Server.writeToLog(e);
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
            Server.writeToLog(e);
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
            Server.writeToLog(e);
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
            Server.writeToLog(e);
        }
    }
}

/**
 * Contains a helper functions for UDP programming
 */
class SocketHelper {
    static final int CONNECT_PORT = 14120;
    static final int INTRODUCER_PORT = 14112;
    static final int PING_PORT = 14110;
    static final int ACK_PORT = 14111;
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