import java.io.*;
import java.net.Socket;
import java.util.*;

class clientFileHandler {
    static final String DIRECTORY = "user.dir";
    static final int BUFFER_SIZE  = 2048;

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

    static void sendFile(String localfilename, Socket socket) throws IOException {
        // Instantiate streams
        File file = new File(getFilePath(localfilename));
        FileInputStream in = new FileInputStream(file);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        long numBytes = file.length();
        out.writeLong(numBytes);

        // Send the file
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((count = in.read(buffer)) > 0) {
            out.write(buffer, 0, count);
        }
    }

    static void receiveFile(String sdfsfilename, Socket socket) throws IOException {
        // Instantiate streams
    	File f = new File(getFilePath(sdfsfilename));
    	boolean exists = f.exists();
        FileOutputStream out = new FileOutputStream(getFilePath(sdfsfilename), exists);
        DataInputStream in = new DataInputStream(socket.getInputStream());

        long numBytes = in.readLong();

        // Receive and write to file
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        if(exists) {
        	String delimiter = "~";
        	out.write(delimiter.getBytes(), 0, delimiter.getBytes().length);
        }
        while ((count = in.read(buffer)) > 0) {
        	System.out.println((new String(buffer, "UTF-8")).indexOf('~'));
            out.write(buffer, 0, count);
            numBytes -= count;
            if (numBytes == 0)
                break;
        }
    }
}

public class Client {

    private static final int SERVER_PORT = 2017;
    static final String[] serverList = {
            "fa18-cs425-g07-01.cs.illinois.edu",
            "fa18-cs425-g07-02.cs.illinois.edu",
            "fa18-cs425-g07-03.cs.illinois.edu",
            "fa18-cs425-g07-04.cs.illinois.edu",
            "fa18-cs425-g07-05.cs.illinois.edu",
            "fa18-cs425-g07-06.cs.illinois.edu",
            "fa18-cs425-g07-07.cs.illinois.edu",
            "fa18-cs425-g07-08.cs.illinois.edu",
            "fa18-cs425-g07-09.cs.illinois.edu",
            "fa18-cs425-g07-10.cs.illinois.edu"
    };
//    static final String[] serverList = {
//            "localhost",
//    };
    private static HashSet<String> commands = new HashSet<>(Arrays.asList(
            "grep",
            "exit",
            "print",
            "quit",
            "log",
            "put localfilename sdfsfilename",
            "get sdfsfilename localfilename",
            "ls sdsfilename",
            "store",
            "delete sdsfilename"
    ));
    private static String lastInput;

    public static String checkCommand(String cmd) {
        String ret = "INVALID";
        String[] components = cmd.split(" ");
        if (components.length == 1) {
            if (components[0].equals("store")) {
                ret = components[0];
            }
        } else if (components.length == 2) {
            if (components[0].equals("delete") || components[0].equals("ls")) {
                ret = cmd;
            }
        } else if (components.length == 3) {
            if (components[0].equals("put") || components[0].equals("get")) {
                if (components[0].equals("put")) {
                    if (!clientFileHandler.fileExists(components[1])) {
                        System.out.print("File does not exist!\n");
                        return ret;
                    }
                }
                ret = cmd;
            }

        }
        return ret;
    }

    private static void execute(String cmd) throws IOException, InterruptedException {
        String[] cmds = cmd.split(" ");
        if (cmds[0].equals("exit")) {
            System.out.println("Quitting...");
            System.exit(0);
        }
        System.out.printf("================= %s =================\n", cmd);
        if (!commands.contains(cmds[0]) && checkCommand(cmd).equals("INVALID")) {
            System.out.printf("Type a valid command from %s%n", commands.toString());
            return;
        }
        if (cmd.equals("store")) {
            FileHandler.printFiles();
        } else {
            ArrayList<queryThread> threads = new ArrayList<>();
            for (String server : serverList) {
                queryThread thread = new queryThread(server, SERVER_PORT, cmd);
                thread.start();
                threads.add(thread);
            }
            for (queryThread thread : threads)
                thread.join();
        }
        System.out.printf("================= %s =================\n", cmd);
        lastInput = new String(cmd);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        @SuppressWarnings("resource")
        Scanner userInput = new Scanner(System.in);
        while (true) {
            System.out.println("Type a Command: ");
            String input = userInput.nextLine().trim();
            // Execute the last input if receive an empty string
            if (input.equals(""))
                execute(lastInput);
            else
                execute(input);
        }
    }

}

/**
 * A query thread represents a single query to a specified ip:port and command
 */
class queryThread extends Thread implements Runnable {

    private String ip;
    private int port;
    private String[] components;
    private Socket socket;
    private BufferedReader reader;
    private DataOutputStream writer;
    private String cmd;

    public queryThread(String ip, int port, String cmd) throws IOException {
        this.cmd = String.format("%s\n", cmd);
        this.components = cmd.split(" ");
        this.ip = ip;
        this.port = port;
    }

    @Override
    public void run() {
        try {
            socket = new Socket(ip, port);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            writer = new DataOutputStream(socket.getOutputStream());
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%s\n", ip));

            // Send command to server
            writer.writeUTF(cmd);

            // Handle extra logic needed by commands
            switch (components[0]) {
                case "put":
                    String localfilename = components[1];
                    FileHandler.sendFile(localfilename, socket);
                    sb.append(String.format("Sent file: %s\n", localfilename));
                    break;
                case "get":
                	clientFileHandler.receiveFile(components[2], socket);
                	sb.append(String.format("Received file: %s\n", components[2]));
                	break;
                default:
                    // Do nothing
                    break;
            }

            // Read response from server
            String line;
            while ((line = reader.readLine()) != null)
                sb.append(String.format("%s\n", line));

            synchronized (System.out) {
                System.out.println(sb.toString());
            }
        } catch (IOException e) {
            System.out.printf("Could not query to %s due to %s\n", ip, e.getMessage());
        }
    }
}
