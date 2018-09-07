package com.jacle.hadoop.hdfs.service;

import com.jacle.hadoop.hdfs.dao.HdfsServiceDao;
import com.jacle.hadoop.hdfs.utils.HdfsUtil;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;

/**
 * Description:
 * author:Jacle
 * Date:2018/9/6
 * Time:8:56
 * <p>
 * （1）：configuration类：此类封装了客户端或服务器的配置，通过配置文件来读取类路径实现（一般是core-site.xml）。
 * （2）：FileSystem类：一个通用的文件系统api，用该对象的一些方法来对文件进行操作。
 * FileSystem fs = FileSystem.get(conf);通过FileSystem的静态方法get获得该对象。
 * （3）：FSDataInputStream：HDFS的文件输入流，FileSystem.open()方法返回的即是此类。
 * （4）：FSDataOutputStream：HDFS的文件输入出流，FileSystem.create()方法返回的即是此类。
 *
 * 除了和下载需要关闭filesystem，其他都不要关闭，关闭了反而容易出错。
 * </p>
 **/
public class HdfsService implements HdfsServiceDao
{

    /**
     * 新建目录
     * @param filepath 目录路径
     */
    @Override
    public boolean createDir(String filepath)
    {
        try
        {
            FileSystem fileSystem = getHadoopFileSystem();
            boolean flag=fileSystem.mkdirs(new Path(HdfsUtil.getConfiguration().get("hdfsURI")+filepath));

            return flag;
        } catch (IOException e)
        {
            e.printStackTrace();
        }

        return false;
    }


    /**
     *
     * @param filepath  目录的路径
     * @param isRecursion  是否递归删除文件
     */
    public boolean delDir(String filepath,boolean isRecursion)
    {
        try
        {
            FileSystem fileSystem = getHadoopFileSystem();
            Path path=new Path(HdfsUtil.getConfiguration().get("hdfsURI")+filepath);
            if(fileSystem.exists(path))
            {
                return fileSystem.delete(path, isRecursion);
            }

        } catch (IOException e)
        {
            e.printStackTrace();
        }

        return false;
    }

    public void createAndWrite(String filepath,String content)
    {
          FSDataOutputStream writer=createFile(filepath, false);
          if(writer!=null)
          {
              try
              {
                  //这里的长度一定要是byte的长度，否则中文会被截断；
                  writer.write(content.getBytes(),0,content.getBytes().length);
//                  writer.close();
              } catch (IOException e)
              {
                  e.printStackTrace();
              }
          }
    }

    @Override
    public void uploadFile(String localPath, String destPath)
    {
        try
        {
            FileSystem fileSystem=getHadoopFileSystem();
            fileSystem.copyFromLocalFile(new Path(localPath), new Path(destPath));

            fileSystem.close();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public FSDataOutputStream createFile(String filepath, boolean isOverride)
    {
        try
        {
            FileSystem fileSystem = getHadoopFileSystem();
            Path path=new Path(HdfsUtil.getConfiguration().get("hdfsURI")+filepath);
            FSDataOutputStream dataOutputStream=fileSystem.create(path,isOverride);

            return dataOutputStream;
        } catch (IOException e)
        {
            e.printStackTrace();
        }

        return null;
    }


    public void renameFile(String oldname,String newname)
    {
        try
        {
            FileSystem fileSystem = getHadoopFileSystem();
            fileSystem.rename(new Path(HdfsUtil.getConfiguration().get("hdfsURI")+"/api_dir2/"+oldname) , new Path(HdfsUtil.getConfiguration().get("hdfsURI")+"/api_dir2/"+newname));

        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void download(String destPath, String localPath)
    {
        try
        {
            FileSystem fileSystem = getHadoopFileSystem();

            fileSystem.copyToLocalFile(new Path(destPath), new Path(localPath));

            fileSystem.close();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void showFileList(String fileDirPath)
    {
        try
        {
            FileSystem fileSystem = getHadoopFileSystem();

           FileStatus[] fileList=fileSystem.listStatus(new Path(fileDirPath));
           for(FileStatus f:fileList)
           {
               System.out.println(f.getPath().toString());
           }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public FileSystem getHadoopFileSystem()
    {
        System.setProperty("hadoop.home.dir", HdfsUtil.getConfiguration().get("hadoop_home"));
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", HdfsUtil.getConfiguration().get("hdfsURI"));
        try
        {
            FileSystem fileSystem = FileSystem.get(URI.create(HdfsUtil.getConfiguration().get("hdfsURI")), configuration);
            return fileSystem;
        } catch (IOException e)
        {
            e.printStackTrace();
        }

        return null;
    }


    /**
     * 读取hdfs的文件，写入到本地文件
     * @param filePath  hdfs文件路径
     */
    public void writeIntoLocalFile(String filePath)
    {
        try
        {
            FileSystem fileSystem = getHadoopFileSystem();
            FSDataInputStream inputStream=fileSystem.open(new Path(filePath));
            FileOutputStream outputStream=new FileOutputStream(new File("d:/file.txt"));

            IOUtils.copy(inputStream, outputStream);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
