/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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

public class HashToAlternateWithSS extends Configured implements Tool {

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

        rounds, numberOfComunications, precomm
    }

    public static class MapMSS extends Mapper<LongWritable, Text, LongPair, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] input = value.toString().split("\t");
            long Vmin, id;
            id = Long.parseLong(input[0]);
            Vmin = id;
            debugPrint("\n Emit " + id + " :", trace);
            boolean flag = false;
            for (String s : input[1].split(",")) {
                if (s.length() > 0) {
                    long u = Long.parseLong(s);
                    if (!flag) {
                        Vmin = Vmin > u ? u : Vmin;
                        flag = false;
                    }
                    if (Vmin != u) {
                        context.write(new LongPair(Vmin, u), new Text(String.valueOf(u)));
                        debugPrint(" [(" + new LongPair(Vmin, u) + "), (" + u + ")]", trace);
                    }
                }
            }
            /*

             if (!input[1].equals(String.valueOf(id))) {

             // ////////////////////////////////EMIT (Vmin,Cv) //////////////////
             context.write(new LongWritable(Vmin), new Text(input[1]));
             context.getCounter(MRrounds.numberOfComunications).increment(1L);
             //debugPrint(" [" + Vmin + ", (" + input[1] + ")]", trace);
             }
             // /////////////////////////////////////
             // ////////////////////////////EMIT (u,Vmin) for all u in Cv
             // ////////////*/
            long u;
            Text vmin = new Text(String.valueOf(Vmin));
            for (String s : input[1].split(",")) {
                if (s.length() > 0) {
                    u = Long.parseLong(s);
                    if (u > Vmin) {
                        context.write(new LongPair(u, Vmin), vmin);
                        context.getCounter(MRrounds.numberOfComunications).increment(1L);
                        debugPrint(" [(" + new LongPair(u, Vmin) + "), " + Vmin + "]", trace);
                    }
                }
            }
        }
    }

    public static class MapSS extends Mapper<LongWritable, Text, LongPair, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // System.setOut(nulled);
            String[] input = value.toString().split("\t");
            long Vmin, id, u;
            id = Long.parseLong(input[0]);
            Vmin = id;
            debugPrint("\n Emit " + id + " :", trace);
            boolean first = true, flag = false;
            for (String s : input[1].split(",")) {
                if (s.length() > 0) {
                    u = Long.parseLong(s);
                    if (first) {
                        Vmin = u;
                        first = false;
                    } else if (u > id) {
                        context.write(new LongPair(Vmin, u), new Text(String.valueOf(u)));
                        debugPrint(" [(" + new LongPair(Vmin, u) + "), (" + u + ")]", trace);
                    }
                }
            }
            //debugPrint("\n Emit " + id + " :", trace);

            /* ////////////////////////////////EMIT (Vmin,Cv) //////////////////
             if (CgtV.toString().length() != 0) {
             context.write(new LongWritable(Vmin), new Text(CgtV.toString()));
             context.getCounter(MRrounds.numberOfComunications).increment(1L);
             //debugPrint(" [" + Vmin + ", (" + CgtV.toString() + ")]", trace);
             }*/
            Text vmin = new Text(String.valueOf(Vmin));
            for (String s : input[1].split(",")) {
                u = Long.parseLong(s);
                if (s.length() > 0 && u > id) {
                    context.write(new LongPair(u, Vmin), vmin);
                    context.getCounter(MRrounds.numberOfComunications).increment(1L);
                    debugPrint(" [(" + new LongPair(u, Vmin) + "), " + Vmin + "]", trace);
                }
            }
        }
    }

    public static class ReduceSS extends Reducer<LongPair, Text, LongWritable, Text> {

        @Override
        public void reduce(LongPair key, Iterable<Text> value, Context context) throws IOException,
                InterruptedException {
            StringBuilder list;
            boolean flag = false;
            list = new StringBuilder();
            long id = key.getFirst();
            Iterator<Text> itr = value.iterator();
            long temp, prev = id;
            
            if (itr.hasNext()) {
                long min = Long.parseLong(itr.next().toString());
                list.append(min);
                prev = min;
            }
            while (itr.hasNext()) {
                temp = Long.parseLong(itr.next().toString());
                if (temp != prev && temp != id) {
                    if(id > prev)
                        flag=true;
                    list.append(",");
                    list.append(temp);
                    prev = temp;
                }
            }
           
            if (flag) {
                context.getCounter(MRrounds.rounds).increment(1L);
            }
            
            context.write(new LongWritable(key.getFirst()), new Text(list.toString()));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        long startTime = System.nanoTime();
        Path inputPath = new Path(args[0]);
        Path basePath = new Path(args[1]);
        Path outputPath = null;
        FileSystem fs = FileSystem.get(getConf());
        fs.delete(basePath, true);
        int iterationCount = 0;
        long terminationValue = 1;
        Job job;
        while (terminationValue > 0) {
            job = jobConfig();
            if (iterationCount % 2 != 0) {
                job.setMapperClass(MapMSS.class);
            } else {
                job.setMapperClass(MapSS.class);
            }
            if (iterationCount != 0) {// for the first iteration the input will
                // be the first input argument
                if (iterationCount > 1) {
                    // fs.delete(inputPath, true);
                }
                inputPath = outputPath;
            }
            outputPath = new Path(basePath, iterationCount + "");
            FileInputFormat.setInputPaths(job, inputPath); // setting the
            FileOutputFormat.setOutputPath(job, outputPath); // setting
            job.waitForCompletion(true); // wait for the job to complete
            Counters jobCntrs = job.getCounters();
            terminationValue = jobCntrs.findCounter(MRrounds.rounds).getValue();
            iterationCount++;
            long comm = jobCntrs.findCounter(MRrounds.numberOfComunications).getValue();
            long precom = jobCntrs.findCounter(MRrounds.precomm).getValue();
            System.out.println("\n Round " + iterationCount + " => #Communications : " + (comm - precom));
            jobCntrs.findCounter(MRrounds.precomm).setValue(comm);
        }
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println(" \nNumber of MR rounds: " + iterationCount + " Number of Communications: "
                + numberOfComunications + " Time of Completion: " + estimatedTime / 1000000000 + "\n");
        return 0;

    }

    protected Job jobConfig() throws IOException {
        JobConf conf = new JobConf();
        Job job = new Job(conf, "iteration");
        job.setJarByClass(HashToAlternateWithSS.class);
        job.setReducerClass(ReduceSS.class);
        job.setPartitionerClass(LongPair.HPartitioner.class);
        job.setSortComparatorClass(LongPair.Comparator.class);
        job.setGroupingComparatorClass(LongPair.GroupComparator.class);
        job.setOutputKeyClass(LongPair.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    public static void main(String args[]) throws Exception {
        System.setErr(nulled);
        int res = ToolRunner.run(new Configuration(), new HashToAlternateWithSS(), args);
        System.exit(res);
    }
}
