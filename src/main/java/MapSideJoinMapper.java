import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class MapSideJoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private Map<Integer, String> customerMapper = new HashMap<>();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        super.setup(context);
        Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());

        for (Path path : paths) {
            if (path.getName().equals("dataset2.txt")) {
                loadHashMap(path, context);
            }
        }

        for (int key : customerMapper.keySet()) {
            System.out.println(key + " - " + customerMapper.get(key));
        }
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] orderTable = value.toString().split(",");
        String textData = orderTable[0] + "\t" + customerMapper.get(Integer.parseInt(orderTable[1]));
        Text finalText = new Text(textData);
        context.write(new IntWritable(Integer.parseInt(orderTable[1])), finalText);
    }


    private void loadHashMap(Path path, Context context) {

        String line = "";
        BufferedReader reader = null;

        try {
            FileSystem fs = FileSystem.get(path.toUri(), context.getConfiguration());
            reader = new BufferedReader(new InputStreamReader(fs.open(path)));

            while ((line = reader.readLine()) != null) {

                String[] customerData = line.split(",");
                customerMapper.put(Integer.parseInt(customerData[0]), customerData[1]);
            }

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            try {
                reader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
