import java.io.*;
import java.net.Socket;

public class FileHandler {
    static final String DIRECTORY = "user.home";
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

    static void sendFile(String localfilename, String sdfsfilename, Socket socket) throws IOException {
        // Capture file
        File file = new File(String.format("%s/%s", System.getProperty(DIRECTORY), localfilename));

        // Collect streams
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
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

    static void receiveFile(String sdfsfilename, Socket socket) throws FileNotFoundException, IOException {
        // Collect streams
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(sdfsfilename));
        BufferedInputStream bis = new BufferedInputStream(socket.getInputStream());

        // Receive and write to file
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((count = bis.read(buffer)) > 0) {
            bos.write(buffer, 0, count);
        }
    }
}
