package chkpt;

import java.util.Collections;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ChkPtAsyncFuncExample extends RichAsyncFunction<Integer, Tuple2<Integer, Boolean>> {
	
	private static final Logger logger = LoggerFactory.getLogger(ChkPtAsyncFuncExample.class);



	@Override
	public void asyncInvoke(Integer value, ResultFuture<Tuple2<Integer, Boolean>> resultFuture) throws Exception {
		logger.info("asyncFunc rcv: {}", value);
		if (value % 10 == 0) {
//			resultFuture.complete(Collections.singletonList(new Tuple2<>(value, value % 2 == 0)));
//			Thread.sleep(10000);
			throw new Exception("kaboom " + value);
		}

		
	
	}
	
}