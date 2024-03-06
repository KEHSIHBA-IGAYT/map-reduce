import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 ************************************
 * The map reduce class starts here *
 ************************************
 */
public class MapReduce {

    /**
     ********************************
     * The mapper class starts here *
     ********************************
     */
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        /**
         ******************************
         * The map method starts here *
         ******************************
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Fetch the path of the dictionary file set in the cache
            Path[] uris = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            String places = uris[0].toString();
            BufferedReader placesContent = new BufferedReader(new FileReader(places));

            // Reading the dictionary content line by line (each line is a place e.g.
            // Australia, India, etc.)
            // At the end of this loop, placesArray list will have all the places as
            // elements
            String line;
            ArrayList<String> placesArray = new ArrayList<>();
            while ((line = placesContent.readLine()) != null) {
                if (line.trim().length() == 0) {
                    continue; // Skip blank lines
                }
                // Add each line to an ArrayList
                placesArray.add(line.toLowerCase());
            }
            placesContent.close();

            // Tokenizing the input file content
            StringTokenizer itr = new StringTokenizer(value.toString());

            // Check if token exists in the "places" list
            String prevToken = "";
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                // Ignore blank tokens
                if (token.trim().length() == 0) {
                    continue;
                }

                // Create a token by appending last token with current one to find places with 2
                // words
                // e.g. New Hamsphire, Fort Worth, San Fransisco, etc.
                if (prevToken != "") {
                    prevToken = prevToken + " " + token;
                    if (placesArray.contains(prevToken.toLowerCase())) {
                        word.set(prevToken);
                        // If token matches any element of the dictionary, emit <token, 1>
                        context.write(word, one);
                    }
                }

                // Check if current token is a geographical reference
                if (placesArray.contains(token.toLowerCase())) {
                    word.set(token);
                    context.write(word, one);
                }

                prevToken = token;
            }
        }
    }

    /**
     *********************************
     * The reducer class starts here *
     *********************************
     */
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        /**
         **********************************
         * The reducer method starts here *
         **********************************
         */
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     **************************************
     * The map reduce program starts here *
     **************************************
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "search places");

        // Setting job parameters
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Setting the dictionary file (places) in the cache to enable mapper to use it
        // later
        DistributedCache.addCacheFile(new URI(args[2]), job.getConfiguration());

        // Providing the directory path where all the input files are present
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Providing the directory path where we want the output to be written
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}