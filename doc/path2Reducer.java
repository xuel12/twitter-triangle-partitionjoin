package twitter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class path2Reducer extends Reducer<Text, Text, Text, Text> {
    //		private final Text result = new Text();
    final String DELIMITER = "_";

    //		@Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        List<String> cache = new ArrayList<>();
        for (Text val : values) {
            System.out.println(key.toString()+':'+val.toString());
            String tmp = val.toString();
            cache.add(tmp);
        }
        System.out.println(cache);
        int size = cache.size();

        for (int i = 0; i < size; i++) {
            String[] tokens1 = cache.get(i).split(DELIMITER);
            for (int j = 0; j < size; j++) {
                String[] tokens2 = cache.get(j).split(DELIMITER);
                String c = tokens2[1];
                String b = key.toString();
                String a = tokens1[1];
                String R1 = tokens1[0];
                String R2 = tokens2[0];
                if (R1.equalsIgnoreCase("R1") && R2.equalsIgnoreCase("R2") &&
                        !a.equalsIgnoreCase(c)) {
                    context.write(new Text(c), new Text("Path2:" + a + '_' + b + '_' + c));
                }
            }
        }
    }
}