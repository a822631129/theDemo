package com.scistor.dshell.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

public class HcatalogHello {

	public static void main(String[] args) throws Exception {
		System.out.println("=========这是一个在Dshell中调用Hcatalog的例子===========");
		Configuration conf = new Configuration();

		String dbName = "default";
		String inputTableName = "student";
		String outputTableName = "student_out";

		Job job = Job.getInstance(conf);
		job.setJobName("MRwithHcatalog");
		// Read the inputTableName table, partition null, initialize the default
		// database.
//		HCatInputFormat.setInput(job, dbName, inputTableName);

		// Initialize HCatoutputFormat
		job.setInputFormatClass(HCatInputFormat.class);
		job.setJarByClass(HcatalogHello.class);
//		job.setMapperClass(Map.class);
//		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(WritableComparable.class);
		job.setOutputValueClass(DefaultHCatRecord.class);

		String inputJobString = job.getConfiguration().get(
				HCatConstants.HCAT_KEY_JOB_INFO);
//		job.getConfiguration().set(HCatConstants.HCAT_KEY_JOB_INFO,
//				inputJobString);

		// Write into outputTableName table, partition null, initialize the
		// default database.
		OutputJobInfo outputJobInfo = OutputJobInfo.create(dbName,
				outputTableName, null);
		job.getConfiguration().set(HCatConstants.HCAT_KEY_OUTPUT_INFO,
				HCatUtil.serialize(outputJobInfo));
//		HCatOutputFormat.setOutput(job, outputJobInfo);

//		HCatSchema s = HCatOutputFormat.getTableSchema(job.getConfiguration());
//		HCatOutputFormat.setSchema(job, s);

		// System.out
		// .println("INFO: Output scheme explicity set for writing:" + s);
		job.setOutputFormatClass(HCatOutputFormat.class);

		System.out.println("=========没有提交job（本地依赖hcatalog成功）============");

	}

}
