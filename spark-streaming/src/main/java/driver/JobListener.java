package driver;
import org.apache.spark.streaming.scheduler.*;

public class JobListener implements StreamingListener {

    @Override
    public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {

        System.out.println("Batch completed, Total delay :" + batchCompleted.batchInfo().totalDelay().get().toString() +  " ms");

    }

    @Override
    public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {

        System.out.println("Batch started from custom listener"+batchStarted.batchInfo());

    }

    @Override
    public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {

    }

    @Override
    public void onReceiverError(StreamingListenerReceiverError receiverError) {

    }

    @Override
    public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {

    }

    @Override
    public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {

    }
   /*
   snipped other methods
   */


}