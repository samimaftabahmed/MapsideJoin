import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class MapSideMain {

    public static void main(String[] args) {

        try {

            Configuration configuration = new Configuration();
            DistributedCache.addCacheFile(new URI(args[0]), configuration);

            Job job = Job.getInstance(configuration);
            job.setJarByClass(MapSideMain.class);
            job.setJobName("Map Side Join");

            FileInputFormat.setInputPaths(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            job.setMapperClass(MapSideJoinMapper.class);
            job.setNumReduceTasks(0);
            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
