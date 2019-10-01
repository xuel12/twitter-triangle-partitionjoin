package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class triangle extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(triangle.class);
    static final Long MAX = 8000L;

    public static class path2Mapper extends Mapper<Object, Text, Text, Text> {
        final String DELIMITER = ",";

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(DELIMITER);
            String follower = tokens[0];
            String followee = tokens[1];
            if (Long.parseLong(follower)<MAX && Long.parseLong(followee)<MAX) {
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
            List<String> cache = new ArrayList<>();
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
                        word.set(tmpstr);
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class path2countMapper extends Mapper<Text, Text, Text, IntWritable> {

        public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
            String val = value.toString();
            IntWritable count = new IntWritable(Integer.parseInt(val));
            context.write(key, count);
        }
    }

    public static class path2countReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class path3Mapper extends Mapper<Text, Text, Text, IntWritable> {

        public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
            String val = value.toString();
            IntWritable count = new IntWritable(Integer.parseInt(val));
            context.write(key, count);
        }
    }

    public static class csvMapper extends Mapper<Object, Text, Text, IntWritable> {
        final String DELIMITER = ",";
        private IntWritable one = new IntWritable(1);

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(DELIMITER);
            String follower = tokens[0];
            String followee = tokens[1];
            if (Long.parseLong(follower)<MAX && Long.parseLong(followee)<MAX) {
                context.write(new Text(followee+'_'+follower), one);
            }
        }
    }

    public static class path3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//        private IntWritable result = new IntWritable();
        public static final String TRIANGLE_COUNTER = "Triangle";

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int sum=0;
            int count=1;
            for(IntWritable val: values) {
                sum+=1;
                count= count * val.get();
                if (sum > 1) {
                    context.getCounter(TRIANGLE_COUNTER, "Number of triangle:").increment(count);
                    break;
                }
            }
//            result.set(count);
//            context.write(key, result);
        }
    }

//    public static class path3countMapper extends Mapper<Text, Text, Text, IntWritable> {
//        public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
//            String val = value.toString();
//            IntWritable count = new IntWritable(Integer.parseInt(val));
//            context.write(new Text("Triangle:"), count);
//        }
//    }
//
//    public static class path3countReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//        private IntWritable result = new IntWritable();
//
//        @Override
//        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
//            int sum=0;
//            for(IntWritable val: values) {
//                sum += val.get();
//            }
//            result.set(sum);
//            context.write(key, result);
//        }
//    }

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

        // job 2: count path2
        Configuration conf2 = getConf();

        final Job job2 = Job.getInstance(conf2, "path2count");
        job2.setJarByClass(triangle.class);

        FileInputFormat.setInputPaths(job2, new Path(args[1]+"/temp1"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/temp2"));
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        job2.setMapperClass(path2countMapper.class);
        job2.setReducerClass(path2countReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);
        controlledJob2.addDependingJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        // job 3: check if path3 existed for each path2 pair
        Configuration conf3 = getConf();

        final Job job3 = Job.getInstance(conf3, "path3");
        job3.setJarByClass(triangle.class);

        MultipleInputs.addInputPath(job3, new Path(args[1] + "/temp2"), KeyValueTextInputFormat.class, path3Mapper.class);
        MultipleInputs.addInputPath(job3, new Path(args[0]), TextInputFormat.class, csvMapper.class);
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/final"));

        job3.setReducerClass(path3Reducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);

        ControlledJob controlledJob3 = new ControlledJob(conf3);
        controlledJob3.setJob(job3);

        // make job3 dependent on job2
        controlledJob3.addDependingJob(controlledJob2);
        // add the job to the job control
        jobControl.addJob(controlledJob3);
//
//        // job 4: count path3
//        Configuration conf4 = getConf();
//
//        final Job job4 = Job.getInstance(conf4, "path3 count");
//        job4.setJarByClass(triangle.class);
//
//        FileInputFormat.setInputPaths(job4, new Path(args[1] + "/temp3"));
//        FileOutputFormat.setOutputPath(job4, new Path(args[1] + "/final"));
//        job4.setInputFormatClass(KeyValueTextInputFormat.class);
//
//        job4.setMapperClass(path3countMapper.class);
//        job4.setReducerClass(path3countReducer.class);
//
//        job4.setOutputKeyClass(Text.class);
//        job4.setOutputValueClass(IntWritable.class);
//
//        ControlledJob controlledJob4 = new ControlledJob(conf4);
//        controlledJob4.setJob(job4);
//
//        // make job4 dependent on job3
//        controlledJob4.addDependingJob(controlledJob3);
//        // add the job to the job control
//        jobControl.addJob(controlledJob4);

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
        int code = job3.waitForCompletion(true) ? 0 : 1;
        if (code == 0) {
            FileWriter fileWriter = new FileWriter(args[1] + "/final/count.txt");

            for (Counter counter : job3.getCounters().getGroup(path3Reducer.TRIANGLE_COUNTER)) {
                String fileContent = counter.getDisplayName() + '\t' + counter.getValue();
                fileWriter.write(fileContent);
                fileWriter.close();
                System.out.println(counter.getDisplayName() + '\t' + counter.getValue());
            }
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
