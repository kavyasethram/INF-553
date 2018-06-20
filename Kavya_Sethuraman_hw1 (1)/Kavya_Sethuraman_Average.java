import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Kavya_Sethuraman_Average{

  public static class TokenizerMapper
       extends Mapper<LongWritable , Text, Text, Text>{

    private Text one = new Text();
    private Text word = new Text();

    public void map(LongWritable  key, Text value, Context context
                    ) throws IOException, InterruptedException {

        try {
            if (key.get() == 0 && value.toString().contains("event") )
                return;
            else {

                String  csv_data = value.toString();
                String[] data_split = csv_data.split(",",-1);
                String arrayOfString_event = data_split[3];


                arrayOfString_event = arrayOfString_event.replaceAll("[^\\p{L}\\p{Nd}]+", " ").replace("^"," ").replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "").replaceAll("\\p{C}", " ").toLowerCase().trim();

                String arrayOfString_pgcnt = data_split[18];

	            if(arrayOfString_event.equals(null) || (arrayOfString_event.equals("")))
	            return;

		        arrayOfString_pgcnt.trim();

                one.set("1" + "," + arrayOfString_pgcnt);
		        word.set(arrayOfString_event);

                //System.out.println(word+"\t\t\t\t\"+one);

		        context.write(word,one);


            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
  }

public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

    Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
        int total = 0;
        int value_1 = 0;
        float value_2 = 0;

        int count = 0;
        float average= 0;


        for (Text value : values) {

        String[] temp_val = value.toString().split(",");
        value_1 = Integer.parseInt(temp_val[0]);
        value_2 = Integer.parseInt(temp_val[1]);
        //sum total and page count
        total+=value_2;
        count+=value_1;
        //computing average
        average= total/count;

        }

        String v = String.valueOf(average) 
                + " " + String.valueOf(count);

        result.set(v);
        context.write(key,result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: average menu <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "average menu");
    job.setJarByClass(Kavya_Sethuraman_Average.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

