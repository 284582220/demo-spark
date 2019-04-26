package com.ygj;
import com.alibaba.fastjson.JSON;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class EsJob {

    private static final Logger log = LoggerFactory.getLogger(EsJob.class);

    private static final Pattern SPACE = Pattern.compile(",");

    public static void main(String[] args) throws IOException {
        if(args.length < 1){
            System.out.println("Usage:<file>");
            System.exit(1);
        }
        // 1、初始化JavaSparkContext
        SparkConf sparkConf = new SparkConf().setAppName("stock-strategy-calculator");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        // 2、加载数据
        JavaRDD<String> lines = context.textFile(args[0], 1);

        // 3、过滤掉空行或不符合格式的数据
        JavaRDD<String> stockInfoRDD = lines.filter(new Function<String, Boolean>(){
            private static final long serialVersionUID = 1L;
            @Override
            public Boolean call(String line) throws Exception{
                String[] stockInfo = SPACE.split(line);
                if(null == stockInfo || stockInfo.length < 8){
                    return false;
                }
                return true;
            }
        });

        if(null != stockInfoRDD){
            // 4、将每行数据转化为RDD
            stockInfoRDD = stockInfoRDD.distinct(); // 数据去重
            JavaPairRDD<String, StockInfo> stockPair = stockInfoRDD.mapToPair(new PairFunction<String, String, StockInfo>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Tuple2<String, StockInfo> call(String line) throws Exception {
                    String[] values = SPACE.split(line);
                    StockInfo stockInfo = new StockInfo();
                    stockInfo.setStockName(values[0]);
                    String stockCode = values[1];
                    stockInfo.setStockCode(stockCode);
                    stockInfo.setTradeDate(values[2]);
                    stockInfo.setHighPrice(Double.valueOf(values[3]));
                    stockInfo.setLowPrice(Double.valueOf(values[4]));
                    return new Tuple2<String, StockInfo>(stockCode, stockInfo);
                }
            });
            // 5、求最值
            JavaPairRDD<String, StockInfo> result = stockPair.reduceByKey(new Function2<StockInfo, StockInfo, StockInfo>() {
                private static final long serialVersionUID = 1L;
                StockInfo temp = null;
                @Override
                public StockInfo call(StockInfo v1, StockInfo v2) throws Exception {
                    temp = new StockInfo();
                    temp.setStockCode(v1.getStockCode());
                    temp.setStockName(v1.getStockName());
                    if(v1.getHighPrice() > v2.getHighPrice()){
                        temp.setHighPrice(v1.getHighPrice());
                        // 若tradeDate为空，说明是比较之后的temp对象,此时应直接取想要的最高价或最低价对应的日期
                        temp.setHighPriceDate(v1.getTradeDate() == null ? v1.getHighPriceDate() : v1.getTradeDate());
                    } else {
                        temp.setHighPrice(v2.getHighPrice());
                        temp.setHighPriceDate(v2.getTradeDate() == null ? v2.getHighPriceDate() : v2.getTradeDate());
                    }
                    if(v1.getLowPrice() < v2.getLowPrice()){
                        temp.setLowPrice(v1.getLowPrice());
                        temp.setLowPriceDate(v1.getTradeDate() == null ? v1.getLowPriceDate() : v1.getTradeDate());
                    }else{
                        temp.setLowPrice(v2.getLowPrice());
                        temp.setLowPriceDate(v2.getTradeDate() == null ? v2.getLowPriceDate() : v2.getTradeDate());
                    }
                    return temp;
                }
            });
            // 6、提取结果输出到ES
            ElasticSearchTools es = new ElasticSearchTools();
            List<Tuple2<String, StockInfo>> output = result.collect();
            for(Tuple2<String, StockInfo> tuple : output){
                System.out.println(tuple._1() + ":" + tuple._2().toString());
                es.insertData("bak_test", JSON.toJSONString(tuple._2()));
            }
            es.closeClinet();
        }

        // 7、关闭context
        context.close();
    }
}
