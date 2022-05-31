package com.xysec.timerservice;

import com.alibaba.fastjson.JSON;
import com.xysec.timerservice.config.ParseConfigByLocalFile;
import com.xysec.timerservice.deserialization.DeserializationSchema;
import com.xysec.timerservice.entity.Customer;
import com.xysec.timerservice.entity.KafkaSourceMessage;
import com.xysec.timerservice.utils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class App {
    public static void main(String[] args) {

        final ParameterTool params = ParameterTool.fromArgs(args);

        //configFile为默认名称
        String configFilePath = params.get("configFile");
        Properties prop = new Properties();
        try {
            prop.load(new StringReader(new String(Files.readAllBytes(Paths.get(configFilePath)), StandardCharsets.UTF_8)));

        } catch (Exception e) {
            prop = new ParseConfigByLocalFile().getConfig(args);
        }

        //开始消费实时数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5*60*1000);
        env.setParallelism(1);

        //设置kafka消费者属性
        Properties kafkaProp = new Properties();
        kafkaProp.put("bootstrap.servers",prop.getProperty("bootstrap.servers"));
        kafkaProp.put("group.id", prop.getProperty("group.id"));


        FlinkKafkaConsumer011<KafkaSourceMessage> timerTopicConsumer =
                new FlinkKafkaConsumer011<>(
                        prop.getProperty("timerTopic"),
                        new DeserializationSchema(),
                        kafkaProp);

        timerTopicConsumer.setStartFromGroupOffsets();

        DataStreamSource<KafkaSourceMessage> ds =
                env.addSource(timerTopicConsumer);

        Properties finalProp = prop;

        ds.map(new MapFunction<KafkaSourceMessage, Customer>() {
            @Override
            public Customer map(KafkaSourceMessage kafkaSourceMessage) throws Exception {
                Customer customer;
                String value = kafkaSourceMessage.value;
                customer = JSON.parseObject(value, Customer.class);
                return customer;
            }
        }).keyBy(new KeySelector<Customer, Object>() {
            @Override
            public Object getKey(Customer customer) throws Exception {
                return customer.getId();
            }
        }).process(new KeyedProcessFunction<Object, Customer, Object>() {

            MapStateDescriptor<String, Customer> stringCustomerMapStateDescriptor;
            QueryRunner queryRunner = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                stringCustomerMapStateDescriptor =
                        new MapStateDescriptor<>("test-id", String.class, Customer.class);
                System.out.println(finalProp.getProperty("idpDruidConfigFile"));
                queryRunner = DbUtils.getQueryRunner(finalProp.getProperty("idpDruidConfigFile"));

            }

            @Override
            public void processElement(Customer customer, KeyedProcessFunction<Object, Customer, Object>.Context context, Collector<Object> collector) throws Exception {

                //注册定时器
                context.timerService().registerProcessingTimeTimer(
                        context.timerService().currentProcessingTime() + 5000
                );
                //collector.collect(customer);
                //拿到mapstate
                MapState<String, Customer> mapState =
                        getRuntimeContext().getMapState(stringCustomerMapStateDescriptor);

                //将需要打宽的customer对象保存进mapState
                mapState.put(context.getCurrentKey().toString(), customer);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Object, Customer, Object>.OnTimerContext ctx, Collector<Object> out) throws Exception {

                //拿到mapState
                MapState<String, Customer> mapState =
                        getRuntimeContext().getMapState(stringCustomerMapStateDescriptor);

                //获取keyby的id
                String id = ctx.getCurrentKey().toString();

                //从mapState中获取保存进去的customer对象
                Customer customer = mapState.get(id);

                String sql = "select name from t1 where id='" + id +  "';";

                //依据id关联维表查询name
                String name = DbUtils.execReturnSingleStringResultQuery(queryRunner, sql);
                customer.setName(name);

                //发射打宽后的customer对象
                out.collect( customer );

                //清空mapState中已经处理过的状态
                mapState.remove(ctx.getCurrentKey().toString());

            }
        })
                .print();


        try {
            env.execute("timer_service");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
