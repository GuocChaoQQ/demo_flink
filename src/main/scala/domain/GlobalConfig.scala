package domain

object GlobalConfig {
  val HBASE_ZOOKEEPER_QUORUM = "node47,node123,node148"
  val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"

  val BOOTSTRAP_SERVERS = "node47:9092,node123:9092,node148:9092"
  val ACKS = "-1"

  val KUDU_MASTER = "node129"
  val KUDU_TABLE_DWDBASEAD = "impala::test.dwd_base_ad"
  val KUDU_TABLE_DWDBASEWEBSITE = "impala::test.dwb_test_web"
  val KUDU_TABLE_DWDVIPLEVEL = "impala::test.dwb_test_vip"
  val KUDU_TABLE_DWSMEMBER = "impala::test.dws_member"
}
