package com.jacle.hadoop;

import com.jacle.hadoop.hdfs.dao.HdfsServiceDao;
import com.jacle.hadoop.hdfs.service.HdfsService;
import org.junit.Before;
import org.junit.Test;

/**
 * Description:
 * author:Jacle
 * Date:2018/9/6
 * Time:14:38
 **/
public class HdfsTester
{
    private HdfsServiceDao hdfsServiceDao;

    @Before
    public void init()
    {
         hdfsServiceDao=new HdfsService();
    }

    @Test
    public void testHdfsCreateDirectory()
    {
        hdfsServiceDao.createDir("/api_dir1");
    }

    @Test
    public void testHdfsDelDirectory()
    {
        hdfsServiceDao.delDir("/api_dir1",true);
    }

    @Test
    public void testHdfsCreateFile()
    {
        //文件目录不存在，会自动创建目录
        hdfsServiceDao.createFile("/api_dir2/file.txt",false);
    }


    @Test
    public void testHdfsCreateAndWriteFile()
    {
        //文件目录不存在，会自动创建目录
        hdfsServiceDao.createAndWrite("/api_dir2/file.txt","这是一个测试创建并读写文件的方法");
    }


    @Test
    public void uploadFile()
    {
       hdfsServiceDao.uploadFile("d:/lxml-4.2.3-cp37-cp37m-win_amd64.whl", "/api_dir2/");
    }

    @Test
    public void rename()
    {
        hdfsServiceDao.renameFile("lxml-4.2.3-cp37-cp37m-win_amd64.whl", "rename_lxml-4.2.3-cp37-cp37m-win_amd64.whl");
    }

    @Test
    public void downloadFile()
    {
        hdfsServiceDao.download("hdfs://10.1.12.201:8020/api_dir2/rename_lxml-4.2.3-cp37-cp37m-win_amd64.whl","d:/" );
    }

    @Test
    public void listFileList()
    {
        hdfsServiceDao.showFileList("hdfs://10.1.12.201:8020/api_dir2/" );
    }

    @Test
    public void writeToLocal()
    {
        hdfsServiceDao.writeIntoLocalFile("hdfs://10.1.12.201:8020/api_dir2/file.txt" );
    }

}
