package baidu.scloud.data.aggregate.client;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.protobuf.Message;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateRequest;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateService;
import org.apache.log4j.Logger;


/**
 * This client class is for invoking the getAvg functions deployed on the
 * Region Server side via the AggregateService. This class will implement the
 * supporting functionality for average the individual results
 * obtained from the AggregateService for each region.
 * <p>
 * This will serve as the client side handler for invoking the aggregate
 * functions.
 * <ul>
 * For all aggregate functions,
 * <li>start row < end row is an essential condition (if they are not
 * {@link HConstants#EMPTY_BYTE_ARRAY})
 * <li>Column family can't be null. In case where multiple families are
 * provided, an IOException will be thrown. An optional column qualifier can
 * also be defined.
 * <li> it returns a Pair<S, Long>. S is sum Column of family and qualifier
 * and Long is row count value
 */
public class AverageCoprocessorClient {
	static Logger logger = Logger.getLogger(AverageCoprocessorClient.class);
	 /**
	   * It computes average while fetching sum and row count from all the
	   * corresponding regions. Approach is to compute a global sum of region level
	   * sum and rowcount and then compute the average.
	   * @param table
	   * @param scan
	   * @throws Throwable
	   */
	public <R, S, P extends Message, Q extends Message, T extends Message>
	  Pair<S, Long> getAvgArgs(final HTable table,
	      final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan) throws Throwable {
	    final AggregateRequest requestArg = validateArgAndGetPB(scan, ci, false);
	    class AvgCallBack implements Batch.Callback<Pair<S, Long>> {
	      S sum = null;
	      Long rowCount = 0l;

	      public Pair<S, Long> getAvgArgs() {
	        return new Pair<S, Long>(sum, rowCount);
	      }

	      public synchronized void update(byte[] region, byte[] row, Pair<S, Long> result) {
	        sum = ci.add(sum, result.getFirst());
	        rowCount += result.getSecond();
	      }
	    }
	    AvgCallBack avgCallBack = new AvgCallBack();
	    table.coprocessorService(AggregateService.class, scan.getStartRow(), scan.getStopRow(),
	        new Batch.Call<AggregateService, Pair<S, Long>>() {
	          public Pair<S, Long> call(AggregateService instance) throws IOException {
	            ServerRpcController controller = new ServerRpcController();
	            BlockingRpcCallback<AggregateResponse> rpcCallback = 
	                new BlockingRpcCallback<AggregateResponse>();
	            instance.getAvg(controller, requestArg, rpcCallback);
	            AggregateResponse response = rpcCallback.get();
	            if (controller.failedOnException()) {
	              throw controller.getFailedOn();
	            }
	            Pair<S, Long> pair = new Pair<S, Long>(null, 0L);
	            if (response.getFirstPartCount() == 0) {
	              return pair;
	            }
	            ByteString b = response.getFirstPart(0);
	            T t = ProtobufUtil.getParsedGenericInstance(ci.getClass(), 4, b);
	            S s = ci.getPromotedValueFromProto(t);
	            pair.setFirst(s);
	            ByteBuffer bb = ByteBuffer.allocate(8).put(
	                getBytesFromResponse(response.getSecondPart()));
	            bb.rewind();
	            pair.setSecond(bb.getLong());
	            return pair;
	          }
	        }, avgCallBack);
	    return avgCallBack.getAvgArgs();
	  }
	  
	  <R, S, P extends Message, Q extends Message, T extends Message> AggregateRequest 
	  validateArgAndGetPB(Scan scan, ColumnInterpreter<R,S,P,Q,T> ci, boolean canFamilyBeAbsent)
	      throws IOException {
	    validateParameters(scan, canFamilyBeAbsent);
	    final AggregateRequest.Builder requestBuilder = 
	        AggregateRequest.newBuilder();
	    requestBuilder.setInterpreterClassName(ci.getClass().getCanonicalName());
	    P columnInterpreterSpecificData = null;
	    if ((columnInterpreterSpecificData = ci.getRequestData()) 
	       != null) {
	      requestBuilder.setInterpreterSpecificBytes(columnInterpreterSpecificData.toByteString());
	    }
	    requestBuilder.setScan(ProtobufUtil.toScan(scan));
	    return requestBuilder.build();
	  }
	  
	  /**
	   * @param scan
	   * @param canFamilyBeAbsent whether column family can be absent in familyMap of scan
	   */
	  private void validateParameters(Scan scan, boolean canFamilyBeAbsent) throws IOException {
		    if (scan == null
		        || (Bytes.equals(scan.getStartRow(), scan.getStopRow()) && !Bytes
		            .equals(scan.getStartRow(), HConstants.EMPTY_START_ROW))
		        || ((Bytes.compareTo(scan.getStartRow(), scan.getStopRow()) > 0) &&
		        	!Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW))) {
		      throw new IOException(
		          "Agg client Exception: Startrow should be smaller than Stoprow");
		    } else if (!canFamilyBeAbsent) {
		      if (scan.getFamilyMap().size() != 1) {
		        throw new IOException("There must be only one family.");
		      }
		    }
		  }
	  
	  byte[] getBytesFromResponse(ByteString response) {
		  ByteBuffer bb = response.asReadOnlyByteBuffer();
		  bb.rewind();
		  byte[] bytes;
		  if (bb.hasArray()) {
		    bytes = bb.array();
		  } else {
		     bytes = response.toByteArray();
		  }
		 return bytes;
	  }
}
