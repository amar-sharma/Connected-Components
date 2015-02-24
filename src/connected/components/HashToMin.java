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
package connected.components;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author ro0t
 */
public class HashToMin extends Configured implements Tool {

    static enum MRrounds {

        rounds
    }

    static long numberOfComunications = 0;
    public static PrintStream nulled = new PrintStream(new OutputStream() {
        @Override
        public void write(int arg0) throws IOException {
            return;
        }
    });

    public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // System.setOut(nulled);
            String[] input = value.toString().split("\t");
            long Vmin, id, u;
            id = Long.parseLong(input[0]);
            Vmin = id;
            boolean flag = false;
            for (String s : input[1].split(",")) {
                if (s.length() > 0) {
                    u = Long.parseLong(s);
                    if (u < Vmin) {
                        flag = true;
                        Vmin = u;
                    }
                }
            }
			// System.out.print("\n Emit " + id + " :");

			// ////////////////////////////////EMIT (Vmin,Cv) //////////////////
            context.write(new LongWritable(Vmin), new Text(input[1]));
            numberOfComunications++;
			// System.out.print(" [" + Vmin + ", (" + input[1] + ")]");

			// /////////////////////////////////////
			// ////////////////////////////EMIT (u,Vmin) for all u in Cv
            // /////////////
            for (String s : input[1].split(",")) {
                u = Long.parseLong(s);
                if (s.length() > 0) {
                    context.write(new LongWritable(u), new Text(String.valueOf(Vmin)));
                    numberOfComunications++;
                    // System.out.print(" [" + s + ", " + Vmin + "]");
                }
            }
            if (flag) {
                // context.getCounter(MRrounds.rounds).increment(1L);
            }
            /*
             * numMaps++; percentage = (long) ((numMaps / 5000000) * 100); if (prevpercentage != percentage) { prevpercentage
             * = percentage; System.out.println(" Map " + percentage + "% Done"); }
             */
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
            /*
             * numRed++; percentage = (long) ((numRed / 5000000) * 100); if (percentage % 5 == 0 && prevpercentage !=
             * percentage) { prevpercentage = percentage; System.out.println(" Reduce " + percentage + "% Done"); }
             */
            // System.out.print("\n " + key + "\t" + list);
        }
    }

    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }

    @Override
    public int run(String[] args) throws Exception {
        long precomm = 0;
        deleteDir(new File("/home/ro0t/Desktop/BTP/output-test-Min"));
        long startTime = System.nanoTime();
        // ... the code being measured ...

        int iterationCount = 0;
        // iterationCount = 21;
        long terminationValue = 1;
        String input, output;
        Job job;
        // args[0] = "/home/ro0t/Desktop/graph/flickr.txt";
        while (terminationValue > 0) {
            job = jobConfig();

            if (iterationCount == 0) // for the first iteration the input will
            // be the first input argument
            {
                input = args[0];
            } else {
                input = args[1] + iterationCount;
                deleteDir(new File(args[1] + (iterationCount - 1)));
            }
            output = args[1] + (iterationCount + 1);
            FileInputFormat.setInputPaths(job, new Path(input)); // setting the
            FileOutputFormat.setOutputPath(job, new Path(output)); // setting
            job.waitForCompletion(true); // wait for the job to complete

            Counters jobCntrs = job.getCounters();
            // Counter jobCntr=job.
            terminationValue = jobCntrs.findCounter(MRrounds.rounds).getValue();
            iterationCount++;
            System.out.println("\n Round " + iterationCount + " => #Communications : " + (numberOfComunications - precomm));
            precomm = numberOfComunications;
        }
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println(" \nNumber of MR rounds: " + iterationCount + " Number of Communications: "
                + numberOfComunications + " Time of Completion: " + estimatedTime / 1000000000 + "\n");

        return 0;

    }

    protected Job jobConfig() throws IOException {
        Job job = new Job(new Configuration(), "iteration");
        job.setJarByClass(HashToMin.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        return job;
    }

    public static void main(String args[]) throws Exception {
        System.setErr(nulled);
        int res = ToolRunner.run(new Configuration(), new HashToMin(), args);
        System.exit(res);
    }
}
