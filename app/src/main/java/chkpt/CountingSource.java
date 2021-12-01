package chkpt;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountingSource implements SourceFunction<Integer> {

	private static final Logger logger = LoggerFactory.getLogger(CountingSource.class);
	private transient volatile boolean isRunning = true;

	@Override
	public void run(SourceContext<Integer> ctx) throws Exception {
		int counter = new Random().nextInt();
		// should be done in open()
		isRunning = true;

		while (isRunning) {
			logger.info("src: {}", counter);
			ctx.collect(counter);
			counter++;
			Thread.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
		logger.info("source cancelled");
	}

}