package com.jacle.hadoop.hdfs.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Description:
 * author:Jacle
 * Date:2018/9/6
 * Time:14:04
 **/
public class HdfsUtil
{
    private static Properties ps=new Properties();
    private static HashMap<String,String> map = new HashMap<String,String>();

    /**
     * 加载配置文件信息
     */
    static
    {
        InputStream inputStream = HdfsUtil.class.getClassLoader().getResourceAsStream("db.properties");
        try
        {
            ps.load(inputStream);
            for(String key:ps.stringPropertyNames())
            {
                map.put(key, ps.getProperty(key));
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public static Map<String,String> getConfiguration()
    {
        return map;
    }

}
