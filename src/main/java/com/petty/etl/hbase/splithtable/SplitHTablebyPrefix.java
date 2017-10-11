package com.petty.etl.hbase.splithtable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;

public class SplitHTablebyPrefix {

	private static Configuration config = null;
	static {
		config = HBaseConfiguration.create();
		config = HBaseConfiguration.addHbaseResources(config);
	}

	public static void main(String[] args) throws Exception {

		config.set("hbase.zookeeper.quorum", "192.168.1.73");

		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.setLong("hbase.rpc.timeout", 60000);
		config.setLong("hbase.client.scanner.caching", 1000);
		config.setLong("hbase.client.keyvalue.maxsize", 524288000);
		config.set("hbase.coprocessor.user.region.classes",
				"org.apache.hadoop.hbase.coprocessor.AggregateImplementation");
		config.set("mapreduce.framework.name", "yarn");
		config.set("yarn.resourcemanager.address", "192.168.1.73" + ":8032");
		config.set("yarn.resourcemanager.scheduler.address", "192.168.1.73" + ":8030");
		System.out.println("Set finished");

		HBaseAdmin admin = new HBaseAdmin(config);
		HTable hTable = new HTable(config, "scrapy_zhihu_newkey");
		HTableDescriptor htd = hTable.getTableDescriptor();
		HTableDescriptor newHtd = new HTableDescriptor(htd);
		newHtd.setValue(HTableDescriptor.SPLIT_POLICY, KeyPrefixRegionSplitPolicy.class.getName());
		newHtd.setValue("prefix_split_key_policy.prefix_length", "1");
		admin.disableTable("scrapy_zhihu_newkey");
		System.out.println("Disable Table");
		admin.modifyTable(Bytes.toBytes("scrapy_zhihu_newkey"), newHtd);
		System.out.println("Modify Table");
		admin.enableTable("scrapy_zhihu_newkey");
		System.out.println("Enable Table");

	}

}
