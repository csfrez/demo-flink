package com.csfrez.flink.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        // 创建流处理执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //executionEnvironment.setParallelism(8);

        // 从文件中读数数据
//        String inputPath = "D:\\Github\\csfrez\\demo-flink\\src\\main\\resources\\hello.txt";
//        DataStreamSource<String> dataStreamSource = executionEnvironment.readTextFile(inputPath);

        // 用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname", "115.159.34.194");
        int port = parameterTool.getInt("port", 8080);

        // 从socket文本流读取数据h
        DataStreamSource<String> dataStreamSource = executionEnvironment.socketTextStream(hostname, port);


        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = dataStreamSource.flatMap(new WordCount.MyFlatMapFunction())
                .keyBy(0)
                .sum(1).setParallelism(2);
        // 打印结果
        resultStream.print().setParallelism(1);

        // 执行任务
        executionEnvironment.execute();
    }
}
