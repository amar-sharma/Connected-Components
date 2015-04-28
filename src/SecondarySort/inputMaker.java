/*
 * The MIT License
 *
 * Copyright 2015 ro0t.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package SecondarySort;

/**
 *
 * @author Amar Sharma
 */
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class inputMaker extends Configured implements Tool {

    static boolean trace = true;
    static long numberOfComunications = 0;
    public static PrintStream nulled = new PrintStream(new OutputStream() {
        @Override
        public void write(int arg0) throws IOException {
        }
    });

    public static void debugPrint(String s, boolean b) {
        if (b) {
            System.out.print(s);
        }
    }

    static enum MRrounds {

        nodes, edges
    }

    public static class MapMSS extends Mapper<LongWritable, Text, LongPair, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] input = value.toString().split("\t");
            long id;
            try {
                id = Long.parseLong(input[0]);
                boolean flag = false;
                for (String s : input[1].split(",")) {
                    if (s.length() > 0) {
                        long u = Long.parseLong(s);
                        context.write(new LongPair(id, u), new Text(String.valueOf(u)));
                        context.write(new LongPair(u, id), new Text(String.valueOf(id)));
                        context.getCounter(MRrounds.edges).increment(1L);
                    }
                }
                context.write(new LongPair(id, id), new Text(String.valueOf(id)));
            } catch (Exception ex) {

            }

        }
    }

    public static class ReduceSS extends Reducer<LongPair, Text, LongWritable, Text> {

        @Override
        public void reduce(LongPair key, Iterable<Text> value, Context context) throws IOException,
                InterruptedException {
            StringBuilder list;
            list = new StringBuilder();
            Iterator<Text> itr = value.iterator();
            long temp, prev = 0;
            if (itr.hasNext()) {
                temp = Long.parseLong(itr.next().toString());
                list.append(temp);
                prev = temp;
            }
            while (itr.hasNext()) {
                temp = Long.parseLong(itr.next().toString());
                if (temp != prev) {
                    list.append(",");
                    list.append(temp);
                    prev = temp;
                }
            }
            context.write(new LongWritable(key.getFirst()), new Text(list.toString()));
            context.getCounter(MRrounds.nodes).increment(1L);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        args[0] = "/home/ro0t/Desktop/BTP/graph/input1.txt";
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(getConf());
        fs.delete(outputPath, true);
        Job job;
        job = jobConfig();
        FileInputFormat.setInputPaths(job, inputPath); // setting the
        FileOutputFormat.setOutputPath(job, outputPath); // setting
        job.waitForCompletion(true); // wait for the job to complete
        Counters jobCntrs = job.getCounters();
        long e = jobCntrs.findCounter(MRrounds.edges).getValue();
        long n = jobCntrs.findCounter(MRrounds.nodes).getValue();
        System.out.println("\n Nodes:" + n + "\tEdges " + e);
        fs.delete(new Path(args[1] + "-final"), true);
        job = jobConfig();
        FileInputFormat.setInputPaths(job, outputPath); // setting the
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "-final")); // setting
        job.waitForCompletion(true); // wait for the job to complete;
        fs.delete(outputPath, true);
        return 0;

    }

    protected Job jobConfig() throws IOException {
        JobConf conf = new JobConf();
        Job job = new Job(conf, "iteration");
        job.setJarByClass(inputMaker.class);
        job.setMapperClass(MapMSS.class);
        job.setReducerClass(ReduceSS.class);
        job.setPartitionerClass(LongPair.HPartitioner.class);
        job.setSortComparatorClass(LongPair.Comparator.class);
        job.setGroupingComparatorClass(LongPair.GroupComparator.class);
        job.setOutputKeyClass(LongPair.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    public static void main(String args[]) throws Exception {
        //System.setErr(nulled);
        int res = ToolRunner.run(new Configuration(), new inputMaker(), args);
        System.exit(res);
    }
}
