import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;


public class Mean {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Kullanım: ortalama <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("Mean");
        job.setJarByClass(Mean.class);
        job.setMapperClass(MeanMapper.class);
        job.setReducerClass(MeanReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCount.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MeanMapper extends Mapper<Object, Text, Text, SumCount> {

        private final int DATE = 0;
        private final int MIN = 1;
        private final int MAX = 2;

        private Map<Text, List<Double>> maxMap = new HashMap<>();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


            String[] values = value.toString().split((","));


            if (values.length != 3) {
                return;
            }


            String date = values[DATE];
            Text month = new Text(date.substring(2));
            Double max = Double.parseDouble(values[MAX]);


            if (!maxMap.containsKey(month)) {
                maxMap.put(month, new ArrayList<Double>());
            }


            maxMap.get(month).add(max);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {


            for (Text month: maxMap.keySet()) {

                List<Double> temperatures = maxMap.get(month);


                Double sum = 0d;
                for (Double max: temperatures) {
                    sum += max;
                }


                context.write(month, new SumCount(sum, temperatures.size()));
            }
        }
    }

    public static class MeanReducer extends Reducer<Text, SumCount, Text, DoubleWritable> {

        private Map<Text, SumCount> sumCountMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {

            SumCount totalSumCount = new SumCount();


            for (SumCount sumCount : values) {


                totalSumCount.addSumCount(sumCount);
            }


            sumCountMap.put(new Text(key), totalSumCount);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {


            for (Text month: sumCountMap.keySet()) {

                double sum = sumCountMap.get(month).getSum().get();
                int count = sumCountMap.get(month).getCount().get();


                context.write(month, new DoubleWritable(sum/count));
            }
        }
    }
}
