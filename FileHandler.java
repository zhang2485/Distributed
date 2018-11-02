import java.io.*;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Comparator;

public class FileHandler {
    static final String USER_DIR = "user.dir";
    static final String SDFS_DIR = "sdfs";
    static final int BUFFER_SIZE  = 1000000;
    static final int REPLICA_PORT = 2018;
    static final int FAILURE_REPLICA_PORT = 5000;
    static final String DELIMITER = "\n--NEW FILE--\n";

    static File[] getFiles() {
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
        return (filename.hashCode() % 10) % Server.group.size();
    }

    static void printFiles() {
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
        for (File f : getFiles()) {
            if (f.getName().equals(filename)) return true;
        }
        return false;
    }

    static String getDirectoryPath() {
        return String.format("%s/%s", System.getProperty(USER_DIR), SDFS_DIR);
    }

    static String getFilePath(String filename) {
        return String.format("%s/%s", getDirectoryPath(), filename);
    }

    static String getNewFilePath(String filename) {
        if (!fileExists(filename))
            new File(getFilePath(filename)).mkdirs();
        return String.format("%s/%s", getFilePath(filename), fileNameSafeString(Server.getCurrentDateAsString()));
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

    static void deleteFile(String filename) {
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
            try {
                Thread.sleep(2000); //2 seconds
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
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

    static File getVersionContent(String filename, int version) throws IOException {
        File[] versions = listVersions(filename);

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

    static void sendAllVersions(String filename, Socket socket) throws IOException {
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeInt(numVersions(filename));
        for (File file : listVersions(filename)) {
            FileInputStream in = new FileInputStream(file);

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
    }

    static void sendAllVersions(File file, Socket socket) throws IOException {
        sendAllVersions(file.getName(), socket);
    }

    static void receiveVersions(String filename, Socket socket) throws IOException {
        DataInputStream in = new DataInputStream(socket.getInputStream());
        int numVersions = in.readInt();
        if (!FileHandler.fileExists(filename))
            new File(getFilePath(filename)).mkdirs();
        for (int i = 0; i < numVersions; i++) {
            receiveFile(new File(getNewFilePath(filename)), socket);
        }
    }


    static void receiveFile(File file, Socket socket) throws IOException {
        if (file.isDirectory())
            file = new File(getNewFilePath(file.getName()));
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

    static void receiveFile(String filename, Socket socket) throws IOException {
        receiveFile(new File(getFilePath(filename)), socket);
    }

}
