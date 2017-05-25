import java.io.*;
import org.apache.hadoop.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.io.*;

public class FindDiff {

	static Configuration conf = null;
	static HTable blog2 = null;

	public static class Mapper extends TableMapper <ImmutableBytesWritable, ImmutableBytesWritable> {
		public Mapper() {}
		@Override
		public void map(ImmutableBytesWritable row, Result values,Context context) throws IOException {
			ImmutableBytesWritable value = null;
			String[] tags = null;
			for (KeyValue kv : values.list()) {
				if ("article".equals(Bytes.toString(kv.getFamily()))	&& "content".equals(Bytes.toString(kv.getQualifier()))) {
					value = new ImmutableBytesWritable(kv.getValue());
				}
				Get g = new Get(row.get());
				Result rs = blog2.get(g);
				ImmutableBytesWritable value2 = new ImmutableBytesWritable(rs.getValue("article".getBytes(), "content".getBytes()));
				if (value.compareTo(value2.get()) != 0) {
					try {
						context.write(row, new ImmutableBytesWritable(new byte[1]));
					} catch (InterruptedException e) {
						throw new IOException(e);
					}
				}
				
// Get the target table with the same row key
// Get the value of 'article:content' of the target row
// compare the two values, if not the same, put KV, else skip it
			}
		}
	}

	public static class Reducer extends TableReducer <ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		public void reduce(ImmutableBytesWritable key,Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
//			Put put = new Put(key.get());
//			put.add(Bytes.toBytes("dummy"), Bytes.toBytes(""), Bytes.toBytes("y"));
			Delete del = new Delete(key.get());
			del.addColumn(Bytes.toBytes("dummy"), Bytes.toBytes(""));
//			context.write(key, put);
			context.write(key, del);
		}
	}

	public static void main(String[] args) throws Exception {
		conf = new Configuration();
		conf = HBaseConfiguration.create(conf);
		blog2 = new HTable(conf, "blog2");
		Job job = new Job(conf, "HBase_DIFF");
		job.setJarByClass(FindDiff.class);
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("article"),Bytes.toBytes("content"));
		TableMapReduceUtil.initTableMapperJob("blog", scan,FindDiff.Mapper.class,	ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
		TableMapReduceUtil.initTableReducerJob("blog_diff",FindDiff.Reducer.class, job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
