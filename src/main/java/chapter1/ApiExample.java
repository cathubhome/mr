package chapter1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created with IDEA
 * Author:catHome
 * Description: hadoop入门之mr api简单学习
 * Time:Create on 2018/8/9 22:53
 */
public class ApiExample {


    public static void main(String[] args) {
//        String fileName = "/usr/catonhometop/wordcount/input/word.txt";
//        readFileIntoConsole(fileName);

        uploadLocalFileIntoHdfs("readme.txt","/usr/catonhometop/read.txt");

    }

    /**
     * 获取fs文件系统
     *
     * @return
     */
    public static FileSystem getFileSystem() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.17.18:8020");
        conf.set("dfs.permissions","false");
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileSystem;
    }

    /**
     * 读取hdfs文件到控制台
     *
     * @param fileName
     */
    public static void readFileIntoConsole(String fileName) {
        FileSystem fileSystem = getFileSystem();
        Path path = new Path(fileName);
        FSDataInputStream fsDataInputStream = null;
        try {
            fsDataInputStream = fileSystem.open(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            IOUtils.copyBytes(fsDataInputStream, System.out, 1024, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fsDataInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 从类路径下上传文件到hdfs文件系统
     * @param localFile
     * @param hdfsFilePath
     */
    public static void uploadLocalFileIntoHdfs(String localFile, String hdfsFilePath) {
        FileSystem fileSystem = getFileSystem();
        InputStream inputsteam = ClassLoader.getSystemResourceAsStream(localFile);
        Path path = new Path(hdfsFilePath);
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = fileSystem.create(path);
            IOUtils.copyBytes(inputsteam, fsDataOutputStream, 1024, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (inputsteam != null) {
                    inputsteam.close();
                }
                if (fsDataOutputStream != null) {
                    fsDataOutputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
