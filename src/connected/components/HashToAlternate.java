/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package connected.components;

/**
 *
 * @author Amar Sharma
 */
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashSet;
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

public class HashToAlternate extends Configured implements Tool {

    static boolean trace = false;
    static long numberOfComunications = 0;
    static long percentage = 0, prevpercentage = -1;
    static float numMaps = 0, numRed = 0;
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

        rounds
    }

    public static class MapM extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] input = value.toString().split("\t");
            long Vmin, id;
            id = Long.parseLong(input[0]);
            Vmin = id;
            boolean flag = false;

            for (String s : input[1].split(",")) {
                if (s.length() > 0) {
                    if (Long.parseLong(s) < Vmin) {
                        flag = true;
                        Vmin = Long.parseLong(s);
                    }
                }
            }
            debugPrint("\n Emit " + id + " :", trace);
            // ////////////////////////////////EMIT (Vmin,Cv) //////////////////
            context.write(new LongWritable(Vmin), new Text(input[1]));
            numberOfComunications++;
            debugPrint(" [" + Vmin + ", (" + input[1] + ")]", trace);
            // /////////////////////////////////////

            // ////////////////////////////EMIT (u,Vmin) for all u in Cv
            // /////////////
            long u;
            for (String s : input[1].split(",")) {
                if (s.length() > 0) {
                    u = Long.parseLong(s);
                    if (u > Vmin) {
                        context.write(new LongWritable(u), new Text(String.valueOf(Vmin)));
                        numberOfComunications++;
                        debugPrint(" [" + s + ", " + Vmin + "]", trace);
                    }
                }
            }
        }
    }

    public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // System.setOut(nulled);
            String[] input = value.toString().split("\t");
            long Vmin, id, u;
            id = Long.parseLong(input[0]);
            Vmin = id;
            boolean first = true, flag = false;
            String CgtV = "" + id;
            for (String s : input[1].split(",")) {
                if (s.length() > 0) {
                    u = Long.parseLong(s);
                    if (u < Vmin) {
                        /*
                         * if (first) { CgtV = String.valueOf(Vmin); first = false; } else CgtV += "," + String.valueOf(Vmin);
                         */
                        flag = true;
                        Vmin = u;
                    } else if (u > id) {
                        if (first) {
                            CgtV = s;
                            first = false;
                        } else {
                            CgtV += "," + s;
                        }
                    }
                }
            }
            debugPrint("\n Emit " + id + " :", trace);

            // ////////////////////////////////EMIT (Vmin,Cv) //////////////////
            if (!CgtV.equals(String.valueOf(id))) {
                context.write(new LongWritable(Vmin), new Text(CgtV));
                numberOfComunications++;
                debugPrint(" [" + Vmin + ", (" + CgtV + ")]", trace);
            }
            for (String s : input[1].split(",")) {
                u = Long.parseLong(s);
                if (s.length() > 0 && u > id) {
                    context.write(new LongWritable(u), new Text(String.valueOf(Vmin)));
                    numberOfComunications++;
                    debugPrint(" [" + s + ", " + Vmin + "]", trace);
                }
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, Text, Text, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException,
                InterruptedException {
            boolean flag = false;
            String list = "";
            long min = Long.parseLong(key.toString());
            long id = min;
            String temp;
            StringBuffer s = new StringBuffer();
            HashSet<String> edges = new HashSet<String>();
            // /////////////////add input to Hashset////////////////
            Iterator<Text> itr = value.iterator();
            while (itr.hasNext()) {
                temp = itr.next().toString();

                for (String splt : temp.split(",")) {
                    if (splt.length() > 0) {
                        edges.add(splt);
                    }
                }
            }

            // ///////////////////////////////find minimum and prepare list///////////
            Iterator<String> set = edges.iterator();
            temp = null;
            long u;
            if (set.hasNext()) {
                temp = set.next();
                s.append(temp);
                u = Long.parseLong(temp);
                if (u < min) {
                    min = u;
                }
            }
            while (set.hasNext()) {
                temp = set.next();
                s.append(",");
                s.append(temp);
                u = Long.parseLong(temp);
                if (u < min) {
                    min = u;
                }
                if (id > min) {
                    flag = true;
                }
            }

            if (flag) {
                context.getCounter(MRrounds.rounds).increment(1L);
            }

            list = s.toString();
            context.write(new Text(String.valueOf(key)), new Text(list));
            debugPrint("\n " + key + "\t" + list, trace);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        long startTime = System.nanoTime();
        long precomm = 0;
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
                job.setMapperClass(MapM.class);
            } else {
                job.setMapperClass(Map.class);
            }
            if (iterationCount != 0) {// for the first iteration the input will
                // be the first input argument
                if (iterationCount > 1) {
                    fs.delete(inputPath, true);
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
            System.out.println("\n Round " + iterationCount + " => #Communications : " + (numberOfComunications - precomm));
            precomm = numberOfComunications;
            percentage = 0;
            numMaps = 0;
            numRed = 0;
        }
        fs.delete(inputPath, true);
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println(" \nNumber of MR rounds: " + iterationCount + " Number of Communications: "
                + numberOfComunications + " Time of Completion: " + estimatedTime / 1000000000 + "\n");
        return 0;

    }

    protected Job jobConfig() throws IOException {
        JobConf conf = new JobConf();
        Job job = new Job(conf, "iteration");
        job.setJarByClass(HashToAlternate.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    public static void main(String args[]) throws Exception {
        System.setErr(nulled);
        int res = ToolRunner.run(new Configuration(), new HashToAlternate(), args);
        System.exit(res);
    }
}
