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

    static void sendFile(String localfilename, String sdfsfilename, Socket socket) throws IOException {
        // Collect streams
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(getFilePath(localfilename)));
        DataOutputStream os = new DataOutputStream(socket.getOutputStream());

        // Notify server of the file that is about to be sent
        os.writeBytes("put " + localfilename + " " + sdfsfilename);

        // Send the file
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((count = bis.read(buffer)) > 0) {
            os.write(buffer, 0, count);
        }
    }

    static void receiveFile(String sdfsfilename, Socket socket) throws IOException {
        // Collect streams
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(getFilePath(sdfsfilename)));
        BufferedInputStream bis = new BufferedInputStream(socket.getInputStream());
        Server.writeToLog('Collected streams');

        // Receive and write to file
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((count = bis.read(buffer)) > 0) {
            Server.writeToLog(buffer);
            bos.write(buffer, 0, count);
        }
    }
}
