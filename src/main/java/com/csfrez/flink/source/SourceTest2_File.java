package com.csfrez.flink.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink从文件中获取数据
 */
public class SourceTest2_File {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使得任务抢占同一个线程
        env.setParallelism(1);

        // 从文件中获取数据输出
        DataStream<String> dataStream = env.readTextFile("D:\\Github\\csfrez\\demo-flink\\src\\main\\resources\\sensor.txt");

        dataStream.print();

        env.execute();
    }
}
