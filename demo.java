package teacherWang;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class demo37 {
        public static class Map extends Mapper<Object, Text, Text, Text> {

                public void map(Object key, Text value, Context context)
                                throws IOException, InterruptedException {
                        String a[] = value.toString().split("\001");
                        ;
                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        ;
                        long t = 0;
                        if (a.length != 16 && a.length != 19)
                                return;
                        if(a[1].compareTo("all") == 0) return;
                        try {
                                Date time = df.parse(a[13].toString());
                                Date s_time = df.parse(a[8].toString());
                                Date e_time = df.parse(a[9].toString());
                                Date start_time = df.parse("2013-07-18 00:00:00");
                                Date current = new Date();
                                if (time.getTime() < start_time.getTime()
                                                || time.getTime() > current.getTime())
                                        return;
                                t = e_time.getTime() - s_time.getTime();
                                if (t > 10 * 60 * 1000L || t <= 0L)
                                        return;
                                if (a[4].compareTo("Wi-Fi") == 0)
                                        return;
                                context.write(new Text(a[1]), new Text(a[10] + "\t" + a[11]));
                        } catch (ParseException parseexception) {
                        }
                }
        }

        public static class Red extends Reducer<Text, Text, Text, Text> {

                public void reduce(Text key, Iterable<Text> value, Context context)
                                throws IOException, InterruptedException {
                        long up_sum = 0;
                        long down_sum = 0;
                        for (Text m : value) {
                                String[] a = m.toString().split("\t");
                                try {
                                        up_sum += Integer.parseInt(a[0].toString());
                                        down_sum += Integer.parseInt(a[1].toString());
                                } catch (Exception e) {
                                        e.printStackTrace();
                                }
                        }
                        if (up_sum + down_sum > 1 * 1000 * 1000 * 1000L)
                                context.write(key, new Text( up_sum + "\t" + down_sum));
                }
        }

        @SuppressWarnings("deprecation")
        public static void main(String args[]) throws IOException,
                        InterruptedException, ClassNotFoundException {
                Configuration conf = new Configuration();
                conf.set("fs.default.name", "hdfs://10.105.34.103:8020");
                String ioArgs[] = {
                                "hdfs://",
                                "demo.out" };
                Job job = new Job(conf, "ambari demo");
                job.setJarByClass(demo37.class);
                job.setMapperClass(Map.class);
                job.setReducerClass(Red.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileSystem fs = FileSystem.get(conf);
                fs.delete(new Path(ioArgs[1]));
                FileInputFormat.addInputPath(job, new Path(ioArgs[0]));
                FileOutputFormat.setOutputPath(job, new Path(ioArgs[1]));
                boolean aaa = job.waitForCompletion(true);
                System.exit(aaa ? 0 : 1);
        }
}
