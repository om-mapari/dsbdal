// Problem Statement 2: 
// UserId|TrackId|Shared|Radio|Skip
// 12345|521|0|1|0
// 34567|521|1|0|0
// 12345|222|0|1|1
// 23456|225|1|0|0

// Number of times the track was shared with others
// Our main focus is to write a mapper class that should emit the track_id and share_counts as intermediate key-value pairs to reducer phase.

// To remember the data sequence in the log file, let's create CONSTANTS class to hold those information.

// CONSTANTS Class to organize data of log:
// package mapreduce;

public class Music_website_constants {
    // This constants class helps to remember the data sequence in the music website
    // log file
    // Here the initialized values are the location of the data to pass in array.
    public static final int USER_ID = 0;
    public static final int TRACK_ID = 1;
    public static final int IS_SHARED = 2;
    public static final int RADIO = 3;
    public static final int IS_SKIPPED = 4;
}

MapReduce Main class:

package mapreduce
;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Music_website_sharecount {
    // enum is used to define collections of constants
    private enum COUNTERS {
        INVALID_RECORD_COUNTS
    }

    // Mapper class to emit intermediate key-value pairs(TRACK_ID,SHARE_COUNT)
    /**
     * Mapper Phase: The text from the input text file is tokenized into words
     * to form a key value pair
     */
    private static class MusicMapper_share extends
            Mapper<Object, Text, IntWritable, IntWritable> {
        // Output object variables
        IntWritable track_id = new IntWritable();
        IntWritable share_id = new IntWritable();

        // Override the map function to add Mapper logic
        @Override
        public void map(Object key, Text value,
                Mapper<Object, Text, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            // Read the input file as line by line
            String line = value.toString();

            // Split the line into tokens, based on delimiter by pipeline
            String tokens[] = line.split("[|]");

            track_id.set(Integer
                    .parseInt(tokens[Music_website_constants.TRACK_ID]));
            share_id.set(Integer
                    .parseInt(tokens[Music_website_constants.IS_SHARED]));
            // Condition to enter if it's shared to make as value for key
            if (share_id.get() == 1) {
                if (tokens.length == 5) {
                    context.write(track_id, share_id);
                } else {
                    // add counter for invalid records
                    context.getCounter(COUNTERS.INVALID_RECORD_COUNTS)
                            .increment(1L);
                }
            }

        }
    }

    // Reducer class: it's an aggregation phase for the keys generated by the
    // map phase
    /**
     * Reducer Phase: In the reduce phase, all the keys are grouped together and
     * the values for similar keys are added up
     */
    // Problem statement : Number of unique listeners per track
    private static class MusicReducer_share extends
            Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        // Override the reduce function to add Reducer logic
        @Override
        public void reduce(
                IntWritable trackid,
                Iterable<IntWritable> share_ids,
                Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            // To avoid holding duplicate user id's , we are using set
            // collection
            Set<Integer> share_idset = new HashSet<>();
            for (IntWritable shareID : share_ids) {
                share_idset.add(shareID.get());
            }
            context.write(trackid, new IntWritable(share_idset.size()));
        }
    }

    // Main method, where the flow of execution and configuration of mapper and
    // reducer
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Music_website_sharecount");

        // Set the Main class name,mapper,reducer to the job
        job.setJarByClass(Music_website_sharecount.class);
        job.setMapperClass(MusicMapper_share.class);
        job.setReducerClass(MusicReducer_share.class);

        // Set outkeyclass and outputvalue class to the job
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // Define the fileInput path and output path format
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}