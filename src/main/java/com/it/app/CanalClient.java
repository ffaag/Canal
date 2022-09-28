package com.it.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;


/**
 * @author ZuYingFang
 * @time 2022-04-24 12:52
 * @description 编写canal的客户端
 */
public class CanalClient {

    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {

        // 获取连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true) {
            // 连接canal
            canalConnector.connect();
            // 订阅数据库，监控test_canal数据库下面所有的表
            canalConnector.subscribe("test_canal.*");
            // 获取数据，单词获取100条sql的数据
            Message message = canalConnector.get(100);
            // 获取entry集合
            List<CanalEntry.Entry> entries = message.getEntries();
            // 判断集合是否为空，如果为空则等待一会儿继续拉取数据
            if (entries.size() <= 0) {
                System.out.println("当次没有数据，休息一会儿");
                Thread.sleep(1000);
            } else {
                // 遍历entries，单条解析
                for (CanalEntry.Entry entry : entries) {
                    // 获取表名
                    String tableName = entry.getHeader().getTableName();
                    // 获取类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    // 或许序列化后的数据
                    ByteString storeValue = entry.getStoreValue();
                    // 判断当前entryType是否为ROWDATA
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        // 反序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        // 获取当前事件的类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        // 获取数据集
                        List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
                        // 遍历打印数据集
                        for (CanalEntry.RowData rowData : rowDataList) {
                            JSONObject beforeData = new JSONObject();
                            for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                                beforeData.put(column.getName(), column.getValue());
                            }

                            JSONObject afterData = new JSONObject();
                            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                                afterData.put(column.getName(), column.getValue());
                            }
                            System.out.println("Table: " + tableName + ", EventType: " + eventType + ", Before: " + beforeData + ", After: " + afterData);
                        }
                    } else {
                        System.out.println("当前数据操作类型为" + entryType);
                    }
                }
            }
        }
    }
}
