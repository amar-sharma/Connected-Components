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

/**
 *
 * @author ro0t
 */
import java.io.IOException;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//Input: Node Vmin Node+neighbourlist
public class HashGreaterToMin extends Configured implements Tool {

    static long numberOfComm = 0,precomm=0;

    static enum MoreIterations {

        numberOfIterations
    }

    public static class MapHashMin extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] input = value.toString().split("\t");
            long id;
            long Vmin;
            boolean flag = false;
            id = Long.parseLong(input[0]);
            Vmin = id;
            for (String s : input[1].split(",")) {
                if (s.length() > 0) {
                    if (Long.parseLong(s) < Vmin) {
                        flag = true;
                        Vmin = Long.parseLong(s);
                    }
                }
            }
            System.out.println("\n Emit " + id + " :");
            context.write(new Text(String.valueOf(id)), new Text(input[1]));
            numberOfComm++;
            System.out.print(" [" + id + ", " + input[1]+ "]");
            for (String s : input[1].split(",")) {
                if (s.length() > 0) {
                    context.write(new Text(String.valueOf(s)), new Text(String.valueOf(Vmin)));
                    System.out.print(" [" + s + ", " + Vmin + "]");
                    numberOfComm++;
                }
            }
        }
    }

    public static class MapHashGreaterToMin extends Mapper<LongWritable, Text, LongWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] input = value.toString().split("\t");
            long Vmin, id, u;
            id = Long.parseLong(input[0]);
            Vmin = id;
            boolean first = true, flag = false;
            StringBuilder CgtV = new StringBuilder();
            for (String s : input[1].split(",")) {
                if (s.length() > 0) {
                    u = Long.parseLong(s);
                    if (u < Vmin) {
                        flag = true;
                        Vmin = u;
                    } else if (u >= id) {
                        if (first) {
                            CgtV.append(s);
                            first = false;
                        } else {
                            CgtV.append(',');
                            CgtV.append(s);
                        }
                    }
                }
            }
            System.out.println("\n Emit " + id + " :");

            // ////////////////////////////////EMIT (Vmin,Cv) //////////////////
            if (CgtV.toString().length() != 0) {
                context.write(new LongWritable(Vmin), new Text(CgtV.toString()));
                numberOfComm++;
                System.out.print(" [" + Vmin + ", (" + CgtV.toString() + ")]");
            }
            for (String s : input[1].split(",")) {
                u = Long.parseLong(s);
                if (s.length() > 0 && u >= id) {
                    context.write(new LongWritable(u), new Text(String.valueOf(Vmin)));
                    numberOfComm++;
                    System.out.print(" [" + s + ", " + Vmin + "]");
                }
            }

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            boolean flag = false;
            String list = "";
            long min = Long.parseLong(key.toString());
            String temp;
            StringBuffer s = new StringBuffer();
            Iterator<Text> itr = value.iterator();
            HashSet<String> edges = new HashSet<String>();
            while (itr.hasNext()) {
                temp = itr.next().toString();
                for (String splt : temp.split(",")) {
                    if (splt.length() > 0) {
                        edges.add(splt);
                        //		System.out.println("Reduce"+key+"\tnbr:"+splt);
                    }
                }
                //			edges.add(itr.next().toString());
            }
            Iterator<String> set = edges.iterator();
            temp = null;
            if (set.hasNext()) {
                temp = set.next();
                s.append(temp);
                //	s.append(",");
                if (Long.parseLong(temp) < min) {
                    min = Long.parseLong(temp);
                    //flag=true;
                }
            }
            while (set.hasNext()) {
                temp = set.next();
                s.append(",");
                s.append(temp);
                if (Long.parseLong(temp) < min) {
                    min = Long.parseLong(temp);
                    flag = true;
                }
                if (Long.parseLong(key.toString()) > min) {
                    flag = true;
                }
            }
            System.out.println(flag);
            if (flag) {
                context.getCounter(MoreIterations.numberOfIterations).increment(1L);
            }
            list = s.toString();
            context.write(new Text(String.valueOf(key)), new Text(list));

        }
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HashGreaterToMin(), args);
        System.exit(res);

    }

    protected Job jobConfig() throws IOException {
        Job job = new Job(new Configuration(), "iteration");
        job.setJarByClass(HashGreaterToMin.class);

        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //	job.setNumReduceTasks(3);
        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        int iterationCount = 0;
        long terminationValue = 1;
        Job job;
        while (terminationValue > 0) {
            job = jobConfig();

            if ((iterationCount + 1) % 3 == 0) {
                job.setMapperClass(MapHashGreaterToMin.class);
            } else {
                job.setMapperClass(MapHashMin.class);
            }

            String input, output;
            if (iterationCount == 0) // for the first iteration the input will be the first input argument
            {
                input = args[0];
            } else // for the remaining iterations, the input will be the output of the previous iteration
            {
                input = args[1] + iterationCount;
            }
            output = args[1] + (iterationCount + 1);
            System.out.println("Input:" + input);
            System.out.println("Output:" + output);
            FileInputFormat.setInputPaths(job, new Path(input)); // setting the input files for the job
            FileOutputFormat.setOutputPath(job, new Path(output)); // setting the output files for the job
            job.waitForCompletion(true); // wait for the job to complete
            Counters jobCntrs = job.getCounters();
            //	Counter jobCntr=job.		
            terminationValue = jobCntrs.findCounter(MoreIterations.numberOfIterations).getValue();
            System.out.println("\n Round " + iterationCount + " => #Communications : " + (numberOfComm - precomm));
            precomm = numberOfComm;
            iterationCount++;
        }
        System.out.println(" Number of MR rounds: " + iterationCount + "\n Number of Communications: " + numberOfComm);
        return 0;

    }

}
