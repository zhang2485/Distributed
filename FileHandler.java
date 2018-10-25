import java.io.*;
import java.net.Socket;

public class FileHandler {
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
        OutputStream out = socket.getOutputStream();

        // Send the file
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((count = in.read(buffer)) > 0) {
            out.write(buffer, 0, count);
        }
        System.out.println("Sent file");
    }

    static void receiveFile(String sdfsfilename, Socket socket) throws IOException {
        // Instantiate streams
        FileOutputStream out = new FileOutputStream(getFilePath(sdfsfilename));
        InputStream in = socket.getInputStream();
        Server.writeToLog("Instantiated streams");

        // Receive and write to file
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((count = in.read(buffer)) > 0) {
            out.write(buffer, 0, count);
        }
        Server.writeToLog("Finished writing to file");
    }
}
