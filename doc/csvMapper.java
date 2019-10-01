package twitter;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class csvMapper extends Mapper<Object, Text, Text, Text> {
    final String DELIMITER = ",";

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(DELIMITER);
        String follower = tokens[0];
        String followee = tokens[1];
        context.write(new Text(followee), new Text("R1_" + follower));
        context.write(new Text(follower), new Text("R2_" + followee));
    }
}