package File;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

public class OutputFile {
    private List<List<String>> contents = new ArrayList<>();
    private File file = null;
    private FileOutputStream osf = null;
    private OutputStreamWriter writer = null;
    private boolean isopen = false;
    private String filename;

    public OutputFile() {
    }

    public OutputFile(String filename) {
        openFile(filename);
    }

    public void openFile(String filename) {
        if (!isopen)
            try {
                this.filename = filename;
                file = new File(filename);
                osf = new FileOutputStream(file);
                writer = new OutputStreamWriter(osf, "UTF-8");
                isopen = true;
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        else {
            System.err.println("Already opened file:" + filename);
        }
    }

    public void print(String str) {
        if (isopen)
            try {
                writer.append(str);
            } catch (IOException e) {
                e.printStackTrace();
            }
        else System.err.println("File is not open,can not print");
    }

    public void println(String str) {
        if (isopen)
            try {
                //window下换行符为\r\n
                writer.append(str + "\r\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        else System.err.println("File is not open,can not println");
    }

    public void println_linux(String str) {
        if (isopen)
            try {
                //linux下换行符为\n
                writer.append(str + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        else System.err.println("File is not open,can not println_linux");
    }

    public void flush() {
        if (isopen)
            try {
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        else System.err.println("File is not open,can not flush");
    }

    public void close() {
        if (isopen)
            try {
                writer.close();
                osf.close();
                isopen = false;
            } catch (IOException e) {
                e.printStackTrace();
            }
        else {
            System.err.println("File is not open,can not close");
        }
    }


}
