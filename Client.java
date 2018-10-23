import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.*;

public class Client {

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

    private static final int SERVER_PORT = 2017;
    private static final String[] serverList = {
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
    private static String lastInput;

    private static void execute(String cmd) throws IOException, InterruptedException {
        String[] cmds = cmd.split(" ");
        if (cmds[0].equals("exit")) {
            System.out.println("Quitting...");
            System.exit(0);
        }
        if (!commands.contains(cmds[0]) && checkCommand(cmd).equals("INVALID")) {
            System.out.printf("Type a valid command from %s%n", commands.toString());
            return;
        }
        System.out.printf("================= %s =================\n", cmd);
        if (cmd.equals("store")) {
            File directory = new File(System.getProperty("user.dir"));
            File[] files = directory.listFiles();
            for (File f : files) {
                System.out.println(f.getName());
            }
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

    private String cmd;
    private String ip;
    private int port;
    private Socket socket;
    private BufferedReader reader;
    private DataOutputStream writer;

    queryThread(String ip, int port, String cmd) throws IOException {
        this.cmd = String.format("%s\n", cmd);
        this.ip = ip;
        this.port = port;
    }

    @Override
    public void run() {
        String[] components = cmd.split(" ");
        try {
            socket = new Socket(ip, port);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            writer = new DataOutputStream(socket.getOutputStream());
            writer.writeBytes(cmd);
            StringBuilder sb = new StringBuilder();
            if (!components[0].equals("ls"))
                sb.append(String.format("%s\n", ip));
            String line;
            while ((line = reader.readLine()) != null)
                sb.append(String.format("%s", line));

            synchronized (System.out) {
                if (!(sb.toString().trim().length() == 0))
                    System.out.println(sb.toString());
            }

        } catch (IOException e) {
            if (!components[0].equals("ls"))
                System.out.printf("Could not query to %s\n", ip);
            return;
        }
    }
}
