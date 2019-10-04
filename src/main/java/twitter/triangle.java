package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;


public class triangle extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(triangle.class);
    static final Integer MAX = 1000;

    public static class path2Mapper extends Mapper<Object, Text, Text, Text> {
        final String DELIMITER = ",";

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(DELIMITER);
            String follower = tokens[0];
            String followee = tokens[1];
            if (Integer.parseInt(follower)<MAX && Integer.parseInt(followee)<MAX) {
                context.write(new Text(followee), new Text("R1_" + follower));
                context.write(new Text(follower), new Text("R2_" + followee));
            }
        }
    }

    public static class path2Reducer extends Reducer<Text, Text, Text, IntWritable> {
        final String DELIMITER = "_";
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
//            Map<String, Integer> path2Cache = new HashMap<>();
            ArrayList<String> cache = new ArrayList<>();
            for (Text val : values) {
                String tmp = val.toString();
                cache.add(tmp);
            }
            int size = cache.size();

            for (int i = 0; i < size; i++) {
                String[] tokens1 = cache.get(i).split(DELIMITER);
                for (int j = 0; j < size; j++) {
                    String[] tokens2 = cache.get(j).split(DELIMITER);
                    String c = tokens2[1];
                    String a = tokens1[1];
                    String R1 = tokens1[0];
                    String R2 = tokens2[0];
                    if (R1.equals("R1") && R2.equals("R2") && !a.equals(c)) {
                        String tmpstr = a +'_'+ c;
//                        path2Cache.put(tmpstr, path2Cache.getOrDefault(tmpstr, 0) + 1);
                        word.set(tmpstr);
                        context.write(word, one);
                    }
                }
            }
//            // partial aggregate over path2 pairs
//            for (String pair : path2Cache.keySet()) {
//                context.write(new Text(pair), new IntWritable(path2Cache.get(pair)));
//            }
        }
    }

    public static class path3Mapper extends Mapper<Text, Text, Text, Text> {

        public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
            String val = "Path2_" + value.toString();
            context.write(key, new Text(val));
        }
    }

    public static class csvMapper extends Mapper<Object, Text, Text, Text> {
        final String DELIMITER = ",";

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(DELIMITER);
            String follower = tokens[0];
            String followee = tokens[1];
            if (Integer.parseInt(follower)<MAX && Integer.parseInt(followee)<MAX) {
                context.write(new Text(followee+'_'+follower), new Text("Edge_1"));
            }
        }
    }

    public static class path3Reducer extends Reducer<Text, Text, Text, IntWritable> {
//        private IntWritable result = new IntWritable();
        public static final String TRIANGLE_COUNTER = "Triangle";
        final String DELIMITER = "_";

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;
            context.getCounter(TRIANGLE_COUNTER, "Number of triangle (not unique):").increment(0);
            int flag = 0;
            for(Text val: values) {
                String[] tokens = val.toString().split(DELIMITER);
                String type = tokens[0];
                Integer count = Integer.parseInt(tokens[1]);
                if (type.equals("Path2")) {
                    sum += count;
                } else {
                    flag = 1;
                }
            }
            if (flag == 1 && sum != 0) {
                context.getCounter(TRIANGLE_COUNTER, "Number of triangle (not unique):").increment(sum);
//                result.set(sum);
//                context.write(key, result);
            }
        }
    }

    public int run(final String[] args) throws Exception {
        JobControl jobControl = new JobControl("jobChain");

        // job one: join edge into path2
        final Configuration conf1 = getConf();
        final Job job1 = Job.getInstance(conf1, "path2");
        job1.setJarByClass(triangle.class);

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/temp1"));

        job1.setMapperClass(path2Mapper.class);
        job1.setReducerClass(path2Reducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        ControlledJob controlledJob1 = new ControlledJob(conf1);
        controlledJob1.setJob(job1);
        jobControl.addJob(controlledJob1);

        // job 2: check if path3 existed for each path2 pair
        Configuration conf2 = getConf();

        final Job job2 = Job.getInstance(conf2, "path3");
        job2.setJarByClass(triangle.class);

        MultipleInputs.addInputPath(job2, new Path(args[1] + "/temp1"), KeyValueTextInputFormat.class, path3Mapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class, csvMapper.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

        job2.setReducerClass(path3Reducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);

        // make job3 dependent on job2
        controlledJob2.addDependingJob(controlledJob1);
        // add the job to the job control
        jobControl.addJob(controlledJob2);

        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

        while (!jobControl.allFinished()) {
            System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());
            System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
            System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
            System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
            System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
            try {
                Thread.sleep(5000);
            } catch (Exception e) {

            }
        }
        int code = job2.waitForCompletion(true) ? 0 : 1;
        if (code == 0) {
            // Create a new file and write data to it.
            FileSystem fileSystem = FileSystem.get(URI.create(args[1] + "/final/"),conf2);
            FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(args[1] + "/final" + "/trianglecount"));
            PrintWriter writer  = new PrintWriter(fsDataOutputStream);

            for (Counter counter : job2.getCounters().getGroup(path3Reducer.TRIANGLE_COUNTER)) {
                String fileContent = "Number of triangle (unique):" + '\t' + counter.getValue()/3;
                writer.write(fileContent);
                System.out.println("Number of triangle (unique):" + '\t' + counter.getValue()/3);
            }
            // Close all the file descriptors
            writer.close();
            fsDataOutputStream.close();
        }
        return (code);
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new triangle(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
        System.exit(0);
    }
}
