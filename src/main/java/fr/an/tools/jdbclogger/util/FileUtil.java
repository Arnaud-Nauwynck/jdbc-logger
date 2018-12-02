package fr.an.tools.jdbclogger.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Collection;

/**
 *
 */
public class FileUtil {

    /** private to force all static.. */
    private FileUtil() {
    }

    /**
     * utility to read a list of String: lines from an input stream
     */
    public static void readAllLines(InputStream inStream, Collection resList) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(inStream));
        for (;;) {
            String line = in.readLine();
            if (line == null) {
                break; // EOF
            }
            line = line.trim();
            if (line.length() > 0) {
                resList.add(line);
            }
        }
    }

    /**
     * utility for readAllLines from a File
     * @return true if file succesfully read
     */
    public static boolean readAllLinesFromFileIfExists(String fileName, Collection resList) {
        boolean res = false;
        if (fileName != null) {
            try {
                File file = new File(fileName);
                if (file.exists()) {
                    InputStream in = null;
                    try {
                        in = new BufferedInputStream(new FileInputStream(file));
                        readAllLines(in, resList);
                        res = true;
                    } catch (Exception ex) {
                        // Debug.severe(FileUtil.class, "readAllLinesFromFileIfExists", "", ex);
                        res = false;
                    } finally {
                        if (in != null) {
                            try {
                                in.close();
                            } catch (Exception ex) {
                            }
                        }
                    }
                }
            } catch (Exception ex) {
            }
        }
        return res;
    }

    /** copy text content to file */
    public static void writeToFile(String text, String fileName) {
        if (fileName == null)
            throw new IllegalArgumentException();
        File file = new File(fileName);
        //            OutputStream out = null;
        //            try {
        //                out = new BufferedOutputStream(new FileOutputStream(file));
        //                if (text == null) text = "";
        //                byte[] content = text.getBytes();
        //                out.write(content);
        //                out.flush();
        //            } catch (Exception ex) {
        //                throw new RuntimeException("failed to write content to file=" + file, ex);
        //            } finally {
        //                if (out != null) try { out.close(); } catch (IOException ex) {}
        //            }

        OutputStreamWriter out = null;
        try {
            out = new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(file)));
            if (text == null)
                text = "";
            out.write(text);
            out.flush();
        } catch (Exception ex) {
            throw new RuntimeException("failed to write content to file=" + file, ex);
        } finally {
            if (out != null)
                try {
                    out.close();
                } catch (IOException ex) {
                }
        }

    }

    /**
     * utility to retrieve extension of a File
     * @return extension or null if file is null or
     * no extension is found
     */
    public static String getExtension(File f) {
        String extension = null;
        if (f != null) {
            String fileName = f.getName();
            int i = fileName.lastIndexOf('.');
            if (i > 0 && i < fileName.length() - 1) {
                extension = fileName.substring(i + 1).toLowerCase();
            }
        }
        return extension;
    }

    public static void copy(String sourceFilePath, String destinationFilePath) {
        try {
            FileInputStream in = null;
            FileOutputStream out = null;
            try {
                in = new FileInputStream(sourceFilePath);
                out = new FileOutputStream(destinationFilePath);

                byte[] buffer = new byte[8 * 1024];
                int count = 0;
                do {
                    out.write(buffer, 0, count);
                    count = in.read(buffer, 0, buffer.length);
                } while (count != -1);
            }

            finally {
                if (out != null) {
                    out.close();
                }
                if (in != null) {
                    in.close();
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
