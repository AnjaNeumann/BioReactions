package org.gradoop.vis;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Scanner;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Created by rostam on 06.07.17.
 *
 */
public class MakeRunFile {
    public static void main(String[] args) {
        Scanner sc = null;
        try {
            sc = new Scanner(new File("run.sh"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String result = "java ";
        while(sc.hasNext()) {
            String s = sc.next();
            if(s.contains("file.encoding")) {
                result += s +" ";
            }
            if(s.contains("idea_rt"))
                continue;
            if(s.contains(":")) {
                result += " -classpath ";
                String[] subparts = s.split(":");
                for(String sp : subparts) {
                    if(sp.contains("/")) {
                        if(!sp.contains("/lib/jvm")) {
                            String tmp = sp.substring(sp.lastIndexOf("/") + 1, sp.length());
                            try {
                                if(!sp.contains("target/classes"))
                                copyFile(new File(sp),new File("target/"+tmp));
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            result += tmp + ":";
                        }
                    }
                }
                result=result.substring(0,result.length()-1);
                result+=" ";
            }
        }
        result+= " org.gradoop.vis.Server";
//        try {
//            FileWriter fw = new FileWriter("target/run.sh");
//            fw.write(result);
//            fw.flush();
//            fw.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        System.out.println(result);
    }

    public static void copyFile(File sourceFile, File destFile) throws IOException {
        if(!destFile.exists()) {
            destFile.createNewFile();
        }

        FileChannel source = null;
        FileChannel destination = null;

        try {
            source = new FileInputStream(sourceFile).getChannel();
            destination = new FileOutputStream(destFile).getChannel();
            destination.transferFrom(source, 0, source.size());
        }
        finally {
            if(source != null) {
                source.close();
            }
            if(destination != null) {
                destination.close();
            }
        }
    }
}
