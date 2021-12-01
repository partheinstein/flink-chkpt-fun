package chkpt;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test {
	
	private static final Logger logger = LoggerFactory.getLogger(Test.class);
	
	public static void main(String[] args) throws Exception {
		MiniClusterWithClientResource flinkCluster =
			      new MiniClusterWithClientResource(
			          new MiniClusterResourceConfiguration.Builder()
			              .setNumberSlotsPerTaskManager(1)
			              .setNumberTaskManagers(1)
			              .build());
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		

		DataStreamSource<Integer> source = env.addSource(new CountingSource());
		DataStream<Tuple2<Integer, Boolean>> stream = AsyncDataStream.unorderedWait(source, new ChkPtAsyncFuncExample(), 1000000, TimeUnit.MILLISECONDS);
		stream.addSink(new SinkFunction<Tuple2<Integer, Boolean>>() {
			@Override
			public void invoke(Tuple2<Integer, Boolean> t, Context context) {
				System.out.println(t);
				//logger.info("sink: {}:{}", t.f0, t.f1);
			}
		});

		env.enableCheckpointing(5000);
		env.setParallelism(1);

		System.out.println("job executing");
		env.execute();
		System.out.println("done");
		
		
		
		
	}

}
