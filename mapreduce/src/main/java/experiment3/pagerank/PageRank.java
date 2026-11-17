package experiment3.pagerank;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public final class PageRank extends Configured implements Tool {
    private static final int ITERATIONS = 10;
    private static final double INITIAL_RANK = 1.0;
    private static final double DAMPING = 0.85;
    private static final double BASE_RANK = 0.15;
    private static final String ROUND_KEY = "experiment3.pagerank.round";
    private static final String LINKS = "links\t";
    private static final String CONTRIBUTION = "contribution\t";

    public static final class RankMapper extends Mapper<LongWritable, Text, Text, Text> {
        private boolean firstRound;

        @Override
        protected void setup(Context context) {
            firstRound = context.getConfiguration().getInt(ROUND_KEY, 0) == 1;
        }

        @Override
        protected void map(LongWritable position, Text record, Context context)
                throws IOException, InterruptedException {
            String[] fields = record.toString().split("\t", -1);
            if (fields.length != (firstRound ? 2 : 3)) {
                throw new IOException("无效的输入记录：" + record);
            }
            String page = fields[0].trim();
            if (page.isEmpty() || page.indexOf(',') >= 0) {
                throw new IOException("无效的网页名：" + record);
            }

            Set<String> targets = new LinkedHashSet<>();
            for (String item : fields[1].split(",", -1)) {
                String target = item.trim();
                if (target.isEmpty() || !targets.add(target)) {
                    throw new IOException("出链为空或重复：" + record);
                }
            }

            double rank = firstRound ? INITIAL_RANK : Double.parseDouble(fields[2]);
            context.write(new Text(page), new Text(LINKS + String.join(",", targets)));
            Text share = new Text(CONTRIBUTION + rank / targets.size());
            for (String target : targets) {
                context.write(new Text(target), share);
            }
        }
    }

    public static final class RankReducer extends Reducer<Text, Text, Object, Text> {
        private boolean finalRound;

        @Override
        protected void setup(Context context) {
            finalRound = context.getConfiguration().getInt(ROUND_KEY, 0) == ITERATIONS;
        }

        @Override
        protected void reduce(Text page, Iterable<Text> messages, Context context)
                throws IOException, InterruptedException {
            String links = null;
            double incoming = 0.0;
            for (Text message : messages) {
                String value = message.toString();
                if (value.startsWith(LINKS)) {
                    if (links != null) {
                        throw new IOException("网页存在多条邻接记录：" + page);
                    }
                    links = value.substring(LINKS.length());
                } else {
                    incoming += Double.parseDouble(value.substring(CONTRIBUTION.length()));
                }
            }
            if (links == null) {
                throw new IOException("链接目标缺少邻接记录：" + page);
            }
            double rank = BASE_RANK + DAMPING * incoming;
            if (finalRound) {
                String line = String.format(Locale.ROOT, "(%s,%.10f)", page, rank);
                context.write(NullWritable.get(), new Text(line));
            } else {
                context.write(page, new Text(links + "\t" + rank));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("用法：PageRank <输入 TSV 路径> <新输出目录>");
            return 2;
        }
        Path previous = new Path(args[0]);
        Path outputRoot = new Path(args[1]);
        if (outputRoot.getFileSystem(getConf()).exists(outputRoot)) {
            System.err.println("输出目录已存在：" + outputRoot);
            return 2;
        }

        for (int round = 1; round <= ITERATIONS; round++) {
            boolean finalRound = round == ITERATIONS;
            String folder = finalRound ? "result" : String.format(Locale.ROOT, "round-%02d", round);
            Path output = new Path(outputRoot, folder);

            Job job = Job.getInstance(getConf(), "PageRank round " + round);
            job.getConfiguration().setInt(ROUND_KEY, round);
            // 中间文本使用制表符连接 key 和 value，供下一轮读取。
            job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "\t");
            job.setJarByClass(PageRank.class);
            job.setMapperClass(RankMapper.class);
            job.setReducerClass(RankReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(finalRound ? NullWritable.class : Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, previous);
            FileOutputFormat.setOutputPath(job, output);

            if (!job.waitForCompletion(true)) {
                return 1;
            }
            if (round == 1 && job.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS)
                    .getValue() == 0) {
                System.err.println("输入图为空");
                return 1;
            }
            previous = output;
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PageRank(), args));
    }
}
