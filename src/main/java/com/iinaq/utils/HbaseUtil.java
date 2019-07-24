package com.iinaq.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName HbaseUtil
 * @Description TODO
 * @Date 2019/7/24  15:50
 **/
public class HbaseUtil {

    private static Configuration conf;

    private static Connection conn;

    private static Admin admin;

    private static Object object = new Object();

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        conf.set("hbase.zookeeper.quorum", "100.0.100.1,100.0.100.2,100.0.100.3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "100.0.100.1:16010");
        try {
            synchronized (object){
                if (conn == null){
                    conn = ConnectionFactory.createConnection(conf);
                    admin = conn.getAdmin();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //创建连接
    public static Connection getConnection(){
        return conn;
    }

    public static Admin getAdmin() {
        return admin;
    }

    //关闭连接
    public static void close(){
        try {
            if (admin != null)
                admin.close();
            if (conn != null)
                conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //创建表
    public static void creatTable(String name,String colFamily) throws Exception {
        TableName tableName = TableName.valueOf(name);
        synchronized (object){
            if (admin.tableExists(tableName)){
                throw new TableExistsException();
            }else {
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(colFamily));
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
                admin.createTable(tableDescriptorBuilder.build());
            }
        }
    }

    /**
     * @description 删除row列族下的某列值
     */
    public static void deleteQualifierValue(String tableName, String rowKey, String family,String qualifier) {

        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Delete delete=new Delete(rowKey.getBytes());
            delete.addColumn(family.getBytes(), qualifier.getBytes());
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * @desc 删除一行
     */
    public static void deleteRow(String tableName, String rowKey, String family) {

        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Delete delete=new Delete(rowKey.getBytes());
            delete.addFamily(family.getBytes());
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * @desc 插入一条记录
     */
    public static void addOneRecord(String tableName, String rowKey, String family, String qualifier, String value){
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),Bytes.toBytes(value));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 添加多条记录
     */
    public static void addMoreRecord(String tableName, String family, String qualifier, List<String> rowList,String value){
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            List<Put> puts=new ArrayList<>();
            Put put = null;
            for (int i = 0; i < rowList.size(); i++) {
                put = new Put(Bytes.toBytes(rowList.get(i)));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),Bytes.toBytes(value));

                puts.add(put);
            }
            table.put(puts);

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * @desc 查询rowkey下某一列值
     */
    public static String getValue(String tableName, String rowKey, String family, String qualifier) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Get get = new Get(rowKey.getBytes());
            get.addColumn(family.getBytes(), qualifier.getBytes());//返回指定列族、列名，避免rowKey下所有数据

            Result rs = table.get(get);
            Cell cell = rs.getColumnLatestCell(family.getBytes(), qualifier.getBytes());

            String value = null;
            if (cell!=null) {
                value = Bytes.toString(CellUtil.cloneValue(cell));
            }

            return value;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     * @desc 获取一行数据
     */
    public static List<Cell> getRowCells(String tableName, String rowKey, String family) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            get.addFamily(family.getBytes());

            Result rs = table.get(get);

            List<Cell> cellList =   rs.listCells();
//    		如果需要,遍历cellList
    		if (cellList!=null) {
    			String qualifier = null;
    			String value = null;
    			for (Cell cell : cellList) {
    				qualifier = Bytes.toString( cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
    				value = Bytes.toString( cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    				System.out.println(qualifier+"--"+value);
    			}
			}
            return cellList;
        } catch (IOException e) {
            if (table!=null){
                try {
                    table.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     * 全表扫描
     * @param tableName 表名
     * @param family 列族名
     * @param qualifier 列名
     * @return
     */
    public static ResultScanner scan(String tableName,String family,String qualifier) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Scan scan = new Scan();
            ResultScanner rs = table.getScanner(scan);
//			一般返回ResultScanner，遍历即可
			if (rs!=null){
				String row = null;
				String quali = null;
    			String value = null;
				for (Result result : rs) {
					row = Bytes.toString(CellUtil.cloneRow(result.getColumnLatestCell(family.getBytes(), qualifier.getBytes())));
					quali =Bytes.toString(CellUtil.cloneQualifier(result.getColumnLatestCell(family.getBytes(), qualifier.getBytes())));
					value =Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(family.getBytes(), qualifier.getBytes())));
					System.out.println(row+"-"+quali+"-"+value);
				}
			}
            return rs;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     * @desc hbase简单分页查询
     * @param tableName
     * @param endRow
     * @param pageSize
     */
    public static List<Result> pageFilter(String tableName, String  endRow,int pageSize){

        ResultScanner scanner = null;
        Table table = null;
        List<Result> list = new ArrayList<>();
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            byte[] POSTFIX = new byte[ 0x00 ];//长度为零的字节数组，0x00十六进制表示0

            Filter filter = new PageFilter(pageSize);
            Scan scan = new Scan();
            scan.setFilter(filter);

            //每次查询的最后一条记录endRow作为新的startRow
            if (endRow!=null){//这里为啥加POSTFIX不是很明白，好像是为了区分下一页，但是我去掉结果也没影响
                byte[] startRow = Bytes.add(Bytes.toBytes(endRow), POSTFIX);
                scan.setStartRow(startRow);
            }
            scanner = table.getScanner(scan);
            Result result;
            while ((result = scanner.next()) != null) {
                list.add(result);
            }
            return list;
        } catch (IOException e){
            e.printStackTrace();
        } finally {
            if (scanner!=null){
                scanner.close();
            }
            if (table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

}

