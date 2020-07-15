package kafka.producer;

import com.alibaba.fastjson.JSON;
import domain.GlobalConfig$;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class BaseMemberPayMoney {
    //创建池
    public static BlockingQueue<KafkaProducer<String, String>> queue = new LinkedBlockingDeque<>(10);
// 支付金额

    public static void main(String[] args) {
        //kafka-topics --zookeeper node47:2181 --create --replication-factor 2 -partitions 1 --topic test_pay
        //kafka-console-consumer --bootstrap-server node123:9092 --topic test_pay --group test  --from-beginning
        Properties props = new Properties();
        props.put("bootstrap.servers",  GlobalConfig$.MODULE$.BOOTSTRAP_SERVERS());
        props.put("acks", "-1");
        props.put("batch.size", "16384");
        props.put("linger.ms", "10");
        props.put("buffer.memory", "33554432");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 1000000; i++) {
            GdmPcenterMemPaymoney memPaymoney = GdmPcenterMemPayMoneyLog.generateLog(String.valueOf(i));
            String jsonString = JSON.toJSONString(memPaymoney);
            producer.send(new ProducerRecord<String, String>("test_pay", jsonString));
        }
        producer.flush();
        producer.close();
    }

    public static class GdmPcenterMemPaymoney {

        private String uid;
        private String paymoney;
        private String vip_id;
        private String updatetime;
        private String siteid;
        private String dt;
        private String dn;
        private String createtime;

        public String getCreatetime() {
            return createtime;
        }

        public void setCreatetime(String createtime) {
            this.createtime = createtime;
        }

        public String getDt() {
            return dt;
        }

        public void setDt(String dt) {
            this.dt = dt;
        }

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public String getPaymoney() {
            return paymoney;
        }

        public void setPaymoney(String paymoney) {
            this.paymoney = paymoney;
        }

        public String getVip_id() {
            return vip_id;
        }

        public void setVip_id(String vip_id) {
            this.vip_id = vip_id;
        }

        public String getUpdatetime() {
            return updatetime;
        }

        public void setUpdatetime(String updatetime) {
            this.updatetime = updatetime;
        }

        public String getSiteid() {
            return siteid;
        }

        public void setSiteid(String siteid) {
            this.siteid = siteid;
        }

        public String getDn() {
            return dn;
        }

        public void setDn(String dn) {
            this.dn = dn;
        }
    }

    public static class GdmPcenterMemPayMoneyLog {

        public static GdmPcenterMemPaymoney generateLog(String uid) {
            GdmPcenterMemPaymoney memPaymoney = new GdmPcenterMemPaymoney();
            Random random = new Random();
            DecimalFormat df = new DecimalFormat("0.00");
            double money = random.nextDouble() * 1000;
            memPaymoney.setPaymoney(df.format(money));
            memPaymoney.setDt(DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now().minusDays(8)));
            memPaymoney.setDn("webA");
            memPaymoney.setSiteid(String.valueOf(random.nextInt(5)));
            memPaymoney.setVip_id(String.valueOf(random.nextInt(5)));
            memPaymoney.setUid(uid);
//            String registerdata = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//                    .format(RondomDate.randomDate("2019-06-30 10:00:00", "2019-06-30 11:00:00"));
            memPaymoney.setCreatetime(String.valueOf(System.currentTimeMillis()));
            return memPaymoney;
        }

    }
}
