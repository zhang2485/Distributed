import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;

public class Client {

    private static final String[] serverList = {
            "fa18-cs425-g77-01.cs.illinois.edu",
            "fa18-cs425-g77-02.cs.illinois.edu",
            "fa18-cs425-g77-03.cs.illinois.edu",
            "fa18-cs425-g77-04.cs.illinois.edu",
            "fa18-cs425-g77-05.cs.illinois.edu",
            "fa18-cs425-g77-06.cs.illinois.edu",
            "fa18-cs425-g77-07.cs.illinois.edu",
            "fa18-cs425-g77-08.cs.illinois.edu",
            "fa18-cs425-g77-09.cs.illinois.edu",
            "fa18-cs425-g77-10.cs.illinois.edu"
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
        "delete sdsfilename",
        "get-versions sdfsfilename num-versions localfilename"
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
        } else if (components.length == 4) {
            if (components[0].equals("get-versions")) {
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
                    if (in.readBoolean()) {
                        sb.append("Received ACK for file!\n");
                        synchronized(lock) {
                            if (!read_quorum) {
                                read_quorum = true;
                                FileHandler.receiveFile(components[2], socket);
                                sb.append("Received file!\n");
                            } else {
                                sb.append("File already ACKED on another query thread\n");
                                socket.close();
                            }
                        }
                    }
                    synchronized (System.out) {
                        System.out.println(sb.toString());
                    }
                    return;
                case "get-versions":
                    if (in.readBoolean()) {
                        sb.append("Received ACK for file!\n");
                        synchronized(lock) {
                            if (!read_quorum) {
                                read_quorum = true;
                                FileHandler.receiveFile(components[3], socket);
                                sb.append("Received file!\n");
                            } else {
                                sb.append("File already ACKED on another query thread\n");
                                socket.close();
                            }
                        }
                    }
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
            sb.append("This node does not hold the replica\n");
        } catch (IOException e) {
            System.out.printf("Could not query to %s due to ", ip);
            e.printStackTrace();
            socket.close();
        }
        synchronized (System.out) {
            System.out.println(sb.toString());
        }

    }
}
