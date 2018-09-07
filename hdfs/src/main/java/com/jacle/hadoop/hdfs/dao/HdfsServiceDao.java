package com.jacle.hadoop.hdfs.dao;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

/**
 * Description:
 * author:Jacle
 * Date:2018/9/6
 * Time:8:55
 **/
public interface HdfsServiceDao
{
    public boolean createDir(String filepath);

    public FileSystem getHadoopFileSystem();

    public boolean delDir(String filepath,boolean isRecursion);

    public FSDataOutputStream createFile(String filepath, boolean isOverride);

    public void createAndWrite(String filepath,String content);

    public void uploadFile(String localPath,String destPath);

    public void renameFile(String oldname,String newname);

    public void download(String destPath,String localPath );

    public void showFileList(String fileDirPath);

    public void writeIntoLocalFile(String filePath);
}
