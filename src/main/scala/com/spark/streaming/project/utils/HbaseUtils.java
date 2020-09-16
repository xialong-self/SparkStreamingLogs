package com.spark.streaming.project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;

/**
 * @author 夏龙
 * @date 2020-08-27
 * Hbase工具类，用于：
 * 连接HBase，获得表实例
 */

public class HbaseUtils {

    private Configuration configuration = null;
    private Connection connection = null;
    private static HbaseUtils instance = null;

    /**
     * 在私有构造方法中初始化属性
     */
    private HbaseUtils(){
        try {
            configuration = new Configuration();
            //指定要访问的zk服务器
            configuration.set("hbase.zookeeper.quorum", "hadoopNode1:2181");
            //得到Hbase连接
            connection = ConnectionFactory.createConnection(configuration);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 获得HBase连接实例
     */

    public static synchronized HbaseUtils getInstance(){
        if(instance == null){
            instance = new HbaseUtils();
        }
        return instance;
    }

    /**
     *由表名得到一个表的实例
     * @param tableName
     * @return
     */
    public HTable getTable(String tableName) {
        HTable hTable = null;
        try {
            hTable = (HTable)connection.getTable(TableName.valueOf(tableName));
        }catch (Exception e){
            e.printStackTrace();
        }
        return hTable;
    }
}
