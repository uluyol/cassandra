package edu.umich.compaction;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.1)",
    comments = "Source: compaction.proto")
public class CoordinatorGrpc {

  private CoordinatorGrpc() {}

  public static final String SERVICE_NAME = "compaction.Coordinator";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<edu.umich.compaction.Coordination.WatchReq,
      edu.umich.compaction.Coordination.ExecCompaction> METHOD_WATCH_COMPACTIONS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "compaction.Coordinator", "WatchCompactions"),
          io.grpc.protobuf.ProtoUtils.marshaller(edu.umich.compaction.Coordination.WatchReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(edu.umich.compaction.Coordination.ExecCompaction.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<edu.umich.compaction.Coordination.UpdateLoadReq,
      edu.umich.compaction.Coordination.Empty> METHOD_UPDATE_LOAD =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "compaction.Coordinator", "UpdateLoad"),
          io.grpc.protobuf.ProtoUtils.marshaller(edu.umich.compaction.Coordination.UpdateLoadReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(edu.umich.compaction.Coordination.Empty.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<edu.umich.compaction.Coordination.QueueCompactionReq,
      edu.umich.compaction.Coordination.Empty> METHOD_QUEUE_COMPACTION =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "compaction.Coordinator", "QueueCompaction"),
          io.grpc.protobuf.ProtoUtils.marshaller(edu.umich.compaction.Coordination.QueueCompactionReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(edu.umich.compaction.Coordination.Empty.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static CoordinatorStub newStub(io.grpc.Channel channel) {
    return new CoordinatorStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static CoordinatorBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new CoordinatorBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static CoordinatorFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new CoordinatorFutureStub(channel);
  }

  /**
   */
  public static abstract class CoordinatorImplBase implements io.grpc.BindableService {

    /**
     */
    public void watchCompactions(edu.umich.compaction.Coordination.WatchReq request,
        io.grpc.stub.StreamObserver<edu.umich.compaction.Coordination.ExecCompaction> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_WATCH_COMPACTIONS, responseObserver);
    }

    /**
     */
    public void updateLoad(edu.umich.compaction.Coordination.UpdateLoadReq request,
        io.grpc.stub.StreamObserver<edu.umich.compaction.Coordination.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_UPDATE_LOAD, responseObserver);
    }

    /**
     */
    public void queueCompaction(edu.umich.compaction.Coordination.QueueCompactionReq request,
        io.grpc.stub.StreamObserver<edu.umich.compaction.Coordination.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_QUEUE_COMPACTION, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_WATCH_COMPACTIONS,
            asyncServerStreamingCall(
              new MethodHandlers<
                edu.umich.compaction.Coordination.WatchReq,
                edu.umich.compaction.Coordination.ExecCompaction>(
                  this, METHODID_WATCH_COMPACTIONS)))
          .addMethod(
            METHOD_UPDATE_LOAD,
            asyncUnaryCall(
              new MethodHandlers<
                edu.umich.compaction.Coordination.UpdateLoadReq,
                edu.umich.compaction.Coordination.Empty>(
                  this, METHODID_UPDATE_LOAD)))
          .addMethod(
            METHOD_QUEUE_COMPACTION,
            asyncUnaryCall(
              new MethodHandlers<
                edu.umich.compaction.Coordination.QueueCompactionReq,
                edu.umich.compaction.Coordination.Empty>(
                  this, METHODID_QUEUE_COMPACTION)))
          .build();
    }
  }

  /**
   */
  public static final class CoordinatorStub extends io.grpc.stub.AbstractStub<CoordinatorStub> {
    private CoordinatorStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CoordinatorStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CoordinatorStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CoordinatorStub(channel, callOptions);
    }

    /**
     */
    public void watchCompactions(edu.umich.compaction.Coordination.WatchReq request,
        io.grpc.stub.StreamObserver<edu.umich.compaction.Coordination.ExecCompaction> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_WATCH_COMPACTIONS, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void updateLoad(edu.umich.compaction.Coordination.UpdateLoadReq request,
        io.grpc.stub.StreamObserver<edu.umich.compaction.Coordination.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_UPDATE_LOAD, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void queueCompaction(edu.umich.compaction.Coordination.QueueCompactionReq request,
        io.grpc.stub.StreamObserver<edu.umich.compaction.Coordination.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_QUEUE_COMPACTION, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class CoordinatorBlockingStub extends io.grpc.stub.AbstractStub<CoordinatorBlockingStub> {
    private CoordinatorBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CoordinatorBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CoordinatorBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CoordinatorBlockingStub(channel, callOptions);
    }

    /**
     */
    public java.util.Iterator<edu.umich.compaction.Coordination.ExecCompaction> watchCompactions(
        edu.umich.compaction.Coordination.WatchReq request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_WATCH_COMPACTIONS, getCallOptions(), request);
    }

    /**
     */
    public edu.umich.compaction.Coordination.Empty updateLoad(edu.umich.compaction.Coordination.UpdateLoadReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_UPDATE_LOAD, getCallOptions(), request);
    }

    /**
     */
    public edu.umich.compaction.Coordination.Empty queueCompaction(edu.umich.compaction.Coordination.QueueCompactionReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_QUEUE_COMPACTION, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class CoordinatorFutureStub extends io.grpc.stub.AbstractStub<CoordinatorFutureStub> {
    private CoordinatorFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CoordinatorFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CoordinatorFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CoordinatorFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<edu.umich.compaction.Coordination.Empty> updateLoad(
        edu.umich.compaction.Coordination.UpdateLoadReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_UPDATE_LOAD, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<edu.umich.compaction.Coordination.Empty> queueCompaction(
        edu.umich.compaction.Coordination.QueueCompactionReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_QUEUE_COMPACTION, getCallOptions()), request);
    }
  }

  private static final int METHODID_WATCH_COMPACTIONS = 0;
  private static final int METHODID_UPDATE_LOAD = 1;
  private static final int METHODID_QUEUE_COMPACTION = 2;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final CoordinatorImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(CoordinatorImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_WATCH_COMPACTIONS:
          serviceImpl.watchCompactions((edu.umich.compaction.Coordination.WatchReq) request,
              (io.grpc.stub.StreamObserver<edu.umich.compaction.Coordination.ExecCompaction>) responseObserver);
          break;
        case METHODID_UPDATE_LOAD:
          serviceImpl.updateLoad((edu.umich.compaction.Coordination.UpdateLoadReq) request,
              (io.grpc.stub.StreamObserver<edu.umich.compaction.Coordination.Empty>) responseObserver);
          break;
        case METHODID_QUEUE_COMPACTION:
          serviceImpl.queueCompaction((edu.umich.compaction.Coordination.QueueCompactionReq) request,
              (io.grpc.stub.StreamObserver<edu.umich.compaction.Coordination.Empty>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    return new io.grpc.ServiceDescriptor(SERVICE_NAME,
        METHOD_WATCH_COMPACTIONS,
        METHOD_UPDATE_LOAD,
        METHOD_QUEUE_COMPACTION);
  }

}
