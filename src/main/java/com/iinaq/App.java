package com.iinaq;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

import static com.iinaq.utils.HbaseUtil.scan;

/**
 * Hello world!
 *
 */
public class App {
    private static final String TABLE_NAME = "zz_store_detail";
    public static final String CLOUMN_FAMILY = "zz";
    public static final String QUALIFIER = "shopId";
    public static void main( String[] args ) {
        String endRow = null;//开始row
        int pageNum = 1;
        int PAGESIZE = 200;//每页须大于1

        List<Result> resultList = null;
        while (true){
            //分页查询t_task_other_mac表
//            resultList=pageFilter(TABLE_NAME,CLOUMN_FAMILY,PAGESIZE);

            scan(TABLE_NAME, CLOUMN_FAMILY, QUALIFIER);
            int size = resultList.size();
            if (size>1){
                Result rr = resultList.get(size-1);
                endRow = Bytes.toString(rr.getRow());
                //如果本页等于pagesize,不是最后一页，移除stopRow,stopRow不包含在本次处理，
                //如果本页小于pagesize表明是最后一页，不再移除stopRow,stopRow包含本次处理
                if (size==PAGESIZE) {
                    resultList.remove(rr);
                }
            }
            System.out.println("=========第"+pageNum+"页==========="+size+"===");
            //遍历Result,进行自己的业务处理
            for (Result r : resultList) {
                System.out.println(Bytes.toString(r.getRow())+":"+r.getColumnCells(Bytes.toBytes("rt"), Bytes.toBytes("name")));
                System.out.println(Bytes.toString(r.getRow())+":"+r.getColumnLatestCell(Bytes.toBytes("rt"), Bytes.toBytes("name")));
            }

            //当每页返回size小于pagesize停止，结束循环
            if (size < PAGESIZE){
                break;
            }
            pageNum++;
        }
    }
}
