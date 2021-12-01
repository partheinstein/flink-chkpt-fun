package chkpt;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * This kind of checkpointing can result in data loss. For example:
 * 1
 * 2
 * 3 > chkpt
 * 4 
 * 5 x fails
 * 1 < recover from chkpt
 * 2
 * 3
 * 
 * If the chkpt freq is high, more data could be lost.
 */
public class ChkPtExampleWithDataLoss extends RichFlatMapFunction<Integer, Tuple2<Integer, Boolean>> implements CheckpointedFunction {
	
	private static final Logger logger = LoggerFactory.getLogger(ChkPtExampleWithDataLoss.class);

	private transient ListState<Integer> checkpointedState;
	private List<Integer> bufferedElements;
	private boolean isRestored;

	ChkPtExampleWithDataLoss() {
		bufferedElements = new ArrayList<>();
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		checkpointedState.clear();
		logger.info("Checkpointing {} elements", bufferedElements.size());
		for (Integer element : bufferedElements) {
			checkpointedState.add(element);
			logger.info("Stored {} to state bkend", element);
		}
		bufferedElements.clear();
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>("buffered-elements",
				TypeInformation.of(new TypeHint<Integer>() {
				}));

		checkpointedState = context.getOperatorStateStore().getListState(descriptor);

		if (context.isRestored()) {
			logger.info("Restoring");
			bufferedElements.clear();
			for (Integer element : checkpointedState.get()) {
				logger.info("Added {}", element);
				bufferedElements.add(element);
			}
			isRestored = true;
		}
	}

	@Override
	public void flatMap(Integer value, Collector<Tuple2<Integer, Boolean>> out) throws Exception {

		bufferedElements.add(value);

		if (isRestored) {
			for (Integer v : bufferedElements) {
				logger.info("map: {}", v);
				out.collect(new Tuple2<>(v, v % 2 == 0));
			}
		}
		
		logger.info("map: {}", value);
		if (value % 10 == 0 && new Random().nextBoolean()) {
			logger.info("About throw ex; bufferedElementSize={}", bufferedElements.size());
			throw new Exception("kaboom " + value);
		}
		
		out.collect(new Tuple2<>(value, value % 2 == 0));


	}
}