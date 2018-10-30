import java.io.*;
import java.net.Socket;

public class FileHandler {
    static final String DIRECTORY = "user.dir";
    static final int BUFFER_SIZE  = 2048;
    static final byte[] DELIMITER = { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
    static final String TMP_FOLDER = "/tmp";

    static File[] getFiles() {
        File directory = new File(System.getProperty(DIRECTORY));
        return directory.listFiles();
    }

    static void printFiles() {
        for (File f : getFiles())
            System.out.println(f.getName());
    }

    static boolean fileExists(String filename) {
        for (File f : getFiles()) {
            if (f.getName().equals(filename)) return true;
        }
        return false;
    }

    static String getFilePath(String filename) {
        return String.format("%s/%s", System.getProperty(DIRECTORY), filename);
    }

    static String getTmpFilePath(String filename) {
        return String.format("%s/%s", TMP_FOLDER, filename);
    }

    static void truncateFile(File file, long numBytes) throws IOException {
        RandomAccessFile raFile = new RandomAccessFile(file, "rwd");
        raFile.setLength(file.length() - numBytes);
    }

    static int numVersions(File file) throws IOException {
        int delimiterIdx = 0;
        byte[] singleByteBuffer = new byte[1];
        int versions = 0;
        FileInputStream in = new FileInputStream(file);
        while (in.read(singleByteBuffer) > 0) {
            if (singleByteBuffer[0] != DELIMITER[delimiterIdx++]) {
                delimiterIdx = 0;
            }
            if (delimiterIdx == DELIMITER.length) {
                versions++;
                delimiterIdx = 0;
            }
        }
        return versions;
    }

    static File getVersionContent(File file, int version) throws IOException {
        byte[] singleByteBuffer = new byte[1];
        int delimiterIdx = 0;
        int currentVersion = 0;

        FileInputStream in = new FileInputStream(file);
        while(currentVersion < version - 1) {
            while (delimiterIdx < DELIMITER.length) {
                // Read byte from file
                in.read(singleByteBuffer);
                if (singleByteBuffer[0] != DELIMITER[delimiterIdx++]) {
                    delimiterIdx = 0;
                }
            }
            currentVersion++;
            delimiterIdx = 0;
        }

        // Read until the next delimiter while writing to tmp file
        String tmpFilePath = getTmpFilePath(file.getName());
        FileOutputStream out = new FileOutputStream(tmpFilePath);
        int numBytes;
        boolean ranIntoDelimiter = false;
        delimiterIdx = 0;
        while ((numBytes = in.read(singleByteBuffer)) > 0) {
            out.write(singleByteBuffer, 0, numBytes);

            if (singleByteBuffer[0] != DELIMITER[delimiterIdx++])
                delimiterIdx = 0;
            else {
                if (delimiterIdx == DELIMITER.length) {
                    ranIntoDelimiter = true;
                    break;
                }
            }
        }
        File tmpFile = new File(tmpFilePath);
        if (ranIntoDelimiter) truncateFile(tmpFile, DELIMITER.length);
        return tmpFile;
    }

    static boolean sendFile(String filename, Socket socket, int version) throws IOException {
        // Instantiate streams
        try {
            File file = getVersionContent(new File(getFilePath(filename)), version);
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

            file.delete();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    static void receiveFile(String filename, Socket socket) throws IOException {
        FileOutputStream out = new FileOutputStream(getFilePath(filename), true);
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
        out.write(DELIMITER, 0, DELIMITER.length);
    }
}
