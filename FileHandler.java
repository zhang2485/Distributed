import java.io.*;
import java.net.Socket;

public class FileHandler {
    static final String USER_DIR = "user.dir";
    static final String SDFS_DIR = "sdfs";
    static final int BUFFER_SIZE  = 2048;
    static final byte[] DELIMITER = { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };

    static File[] getFiles() {
        File directory = new File(getDirectoryPath());
        return directory.listFiles();
    }

    static String getMasterNodeIP() {
        return Server.group.get(0);
    }

    static int getFileNode(String filename) {
        return filename.hashCode() % Server.group.size();
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

    static String getDirectoryPath() {
        return String.format("%s/%s", System.getProperty(USER_DIR), SDFS_DIR);
    }

    static String getFilePath(String filename) {
        return String.format("%s/%s", getDirectoryPath(), filename);
    }

    static void deleteFile(String filename) {
        new File(getFilePath(filename)).delete();
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
        File tmpFile = File.createTempFile(file.getName(), "");
        tmpFile.deleteOnExit();
        FileOutputStream out = new FileOutputStream(tmpFile);
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
        in.close();
        if (ranIntoDelimiter) truncateFile(tmpFile, DELIMITER.length);
        return tmpFile;
    }

    static void sendFile(String filename, Socket socket, int version) throws IOException {
        // Instantiate streams
        File file = getVersionContent(new File(getFilePath(filename)), version);
        FileInputStream in = new FileInputStream(file);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        long numBytes = file.length();
        // Handle empty files by throwing an exception
        if (numBytes <= 0) throw new IOException();
        out.writeLong(numBytes);

        // Send the file
        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((count = in.read(buffer)) > 0) {
            out.write(buffer, 0, count);
        }
    }

    static void receiveFile(String filename, Socket socket, boolean append) throws IOException {
        FileOutputStream out = new FileOutputStream(getFilePath(filename), append);
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
        if (append)
            out.write(DELIMITER, 0, DELIMITER.length);
    }
}
