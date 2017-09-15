import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class FindFrequency {

    public static class mapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static Set<String> words = new HashSet<String>();

        @Override
        public void setup(Context context) throws IOException {
            if (context.getCacheFiles() != null
                    && context.getCacheFiles().length > 0) {
                String filename = context.getCacheFiles()[0].toString();
                //System.out.println(filename);
                BufferedReader br = new BufferedReader(new FileReader(new File("words   ")));
                String line = "";
                while((line=br.readLine())!=null){
                    String[] temp = line.trim().split("\\s+");
                    words.add(temp[0]);
                }
                br.close();
            }
        }
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\\s+");
            for(int i=0;i<strs.length;i++){
                String temp = strs[i].trim();
                if(temp.length()!=0){
                    temp = removeQM(temp);
                    if(words.contains(temp)){
                        word.set(temp);
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class reducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static String removeQM(String s){
        String result = s;
        while(result.startsWith("'")){
            result = result.substring(1, result.length());
        }
        while(result.endsWith("'")){
            result = result.substring(0, result.length()-1);
        }
        return result;
    }
}