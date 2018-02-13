package Python2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class RunPy {
    private Process pr = null;
    private String FilePathandName = null;
    private boolean isinit = false;

    public RunPy() {
    }

    ;

    public RunPy(String file) {
        this.SetFile(file);
        isinit = true;
    }

    public void SetFile(String file) {
        this.FilePathandName = file;
    }

    public void RunWithArgs(String[] args) throws IOException, InterruptedException {
        if (isinit) {
            try {
                List<String> listargs = new ArrayList<String>();
                listargs.add("python");
                listargs.add(this.FilePathandName);
                for (String temp : args) {
                    listargs.add(temp);
                }
                pr = Runtime.getRuntime().exec((String[]) listargs.toArray());
                BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
                String line;
                while ((line = in.readLine()) != null) {
                    System.out.println(line);
                }
                in.close();
                pr.waitFor();
                System.out.println("end");
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.err.println("未设置文件");
        }
    }

    public void Run() {
        if (isinit) {
            try {
                ArrayList<String> listargs = new ArrayList<String>();
                listargs.add("python");
                listargs.add(this.FilePathandName);
                String[] str = new String[listargs.size()];
                pr = Runtime.getRuntime().exec(str);
                BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
                String line;
                while ((line = in.readLine()) != null) {
                    System.out.println(line);
                }
                in.close();
                pr.waitFor();
                System.out.println("end");
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.err.println("未设置文件");
        }
    }

    public void destroy() {
        isinit = false;
    }
}
