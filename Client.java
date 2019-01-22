import java.io.*;
import java.net.Socket;
import java.net.SocketException;
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

    static void sendFile(String filename, Socket socket) throws IOException {
        // Instantiate streams
        File file = new File(getFilePath(filename));
        FileInputStream in = new FileInputStream(file);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        long numBytes = file.length();
        out.writeLong(numBytes);

        // Send the file
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((count = in.read(buffer)) > 0) {
            out.write(buffer, 0, count);
            if(in.available() == 0) {
            	System.out.println("45");
            	break;
            }
        }
    }

    static void receiveFile(String filename, Socket socket) throws IOException {
        // Instantiate streams
        FileOutputStream out = null;
        DataInputStream in = new DataInputStream(socket.getInputStream());

        long numBytes = in.readLong();

        // Receive and write to file
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        boolean first = true;
        while ((count = in.read(buffer)) > 0) {
        	if(first) {
        		first = false;
        		String check = new String(buffer);
        		if(check.trim().equals("DNE")) {
        			System.out.println("DNE SUCCEEDED");
        			break;
        		}
        		out = new FileOutputStream(getFilePath(filename));
        	}
            out.write(buffer, 0, count);
            numBytes -= count;
            if (numBytes == 0)
                break;
        }
    }
}

public class Client {

    };
//    static final String[] serverList = {
//            "localhost",
//    };
    private static HashSet<String> commands = new HashSet<>(Arrays.asList(
    ));
    private static String lastInput;

    // Checks the input string against our dictionary of existing commands
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
        long startTime = System.currentTimeMillis();
        if (!commands.contains(cmds[0]) && checkCommand(cmd).equals("INVALID")) {
            System.out.printf("Type a valid command from %s%n", commands.toString());
            return;
        }
        if (cmd.equals("store")) {
            FileHandler.printFiles();
        } else {
            ArrayList<queryThread> threads = new ArrayList<>();
            queryThread.read_quorum = false;
            for (String server : serverList) {
                queryThread thread = new queryThread(server, Server.SERVER_PORT, cmd);
                thread.start();
                threads.add(thread);
            }
            for (queryThread thread : threads)
                thread.join();
        }
        long endTime = System.currentTimeMillis();
        System.out.printf("================= %s %dms =================\n", cmd, endTime - startTime);
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
    static private final Object lock = new Object();
    static boolean read_quorum;

    public queryThread(String ip, int port, String cmd) throws IOException {
        this.cmd = String.format("%s\n", cmd);
        this.components = cmd.split(" ");
        this.ip = ip;
        this.port = port;
    }

    @Override
    public void run() {
        StringBuilder sb = new StringBuilder();
        try {
            socket = new Socket(ip, port);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            DataInputStream in = new DataInputStream(socket.getInputStream());
            writer = new DataOutputStream(socket.getOutputStream());
            sb.append(String.format("%s\n", ip));

            // Send command to server
            writer.writeUTF(cmd);

            // Handle extra logic needed by commands
            switch (components[0]) {
                case "put":
                    FileHandler.sendFile(components[1], socket, 0);
                    break;
                case "get":
                    synchronized (System.out) {
                        System.out.println(sb.toString());
                    }
                    return;
                case "get-versions":
                    synchronized (System.out) {
                        System.out.println(sb.toString());
                    }
                    return;
                default:
                    // Do nothing
                    break;
            }

            // Read response from server
            String line;
            while ((line = reader.readLine()) != null)
                sb.append(String.format("%s\n", line));

            socket.close();
        } catch (EOFException e) {
            sb.append("Server closed socket signalling DNE\n");
        } catch (SocketException e) {
            switch (components[0]) {
                case "put":
                    sb.append("This node does not hold the replica\n");
                    break;
                default:
                    sb.append(String.format("Could not connect to %s\n", ip));
                    break;
            }
        } catch (IOException e) {
            System.out.printf("Could not query %s due to ", ip);
            e.printStackTrace();
        }
        synchronized (System.out) {
            System.out.println(sb.toString());
        }

    }
}
