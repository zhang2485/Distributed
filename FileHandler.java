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
        // Get size of file in bytes
        File file = new File(getFilePath(localfilename));
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        dos.writeLong(file.length());

        // Instantiate streams
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
        BufferedOutputStream bos = new BufferedOutputStream(socket.getOutputStream());

        // Send the file
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((count = bis.read(buffer)) > 0) {
            bos.write(buffer, 0, count);
            bos.flush();
        }
    }

    static void receiveFile(String sdfsfilename, Socket socket) throws IOException {
        // Instantiate streams
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(getFilePath(sdfsfilename)));
        BufferedInputStream bis = new BufferedInputStream(socket.getInputStream());
        DataInputStream dis = new DataInputStream(socket.getInputStream());
        long numBytes = dis.readLong();

        // Receive and write to file
        byte[] buffer = new byte[BUFFER_SIZE];
        while (numBytes > 0) {
            Server.writeToLog(String.format("%d bytes left to accept", numBytes));

            // Read data from stream
            int count = bis.read(buffer);
            numBytes -= count;
            bos.write(buffer, 0, count);
            bos.flush();
        }
        Server.writeToLog("Finished writing to file");
    }
}
