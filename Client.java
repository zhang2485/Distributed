import java.io.*;
import java.net.Socket;
import java.util.*;

public class Client {

    private static final int SERVER_PORT = 2017;
    static final String[] serverList = {
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
                    if (!FileHandler.fileExists(components[1])) {
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
                    FileHandler.sendFile(components[1], socket);
                    break;
                case "get":
                    FileHandler.receiveFile(components[2], socket);
                    sb.append("Received file!\n");
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

            synchronized (System.out) {
                System.out.println(sb.toString());
            }

        } catch (IOException e) {
            System.out.printf("Could not query to %s due to %s\n", ip, e.getMessage());
        }
    }
}
