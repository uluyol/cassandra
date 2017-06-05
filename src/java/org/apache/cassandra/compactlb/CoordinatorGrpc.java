package org.apache.cassandra.compactlb;

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
  public static final io.grpc.MethodDescriptor<org.apache.cassandra.compactlb.Coordination.RegisterReq,
      org.apache.cassandra.compactlb.Coordination.Empty> METHOD_REGISTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "compaction.Coordinator", "Register"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.apache.cassandra.compactlb.Coordination.RegisterReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.apache.cassandra.compactlb.Coordination.Empty.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<org.apache.cassandra.compactlb.Coordination.WatchReq,
      org.apache.cassandra.compactlb.Coordination.WatchResp> METHOD_WATCH =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "compaction.Coordinator", "Watch"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.apache.cassandra.compactlb.Coordination.WatchReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.apache.cassandra.compactlb.Coordination.WatchResp.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<org.apache.cassandra.compactlb.Coordination.UpdateReq,
      org.apache.cassandra.compactlb.Coordination.Empty> METHOD_UPDATE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "compaction.Coordinator", "Update"),
          io.grpc.protobuf.ProtoUtils.marshaller(org.apache.cassandra.compactlb.Coordination.UpdateReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(org.apache.cassandra.compactlb.Coordination.Empty.getDefaultInstance()));

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
    public void register(org.apache.cassandra.compactlb.Coordination.RegisterReq request,
        io.grpc.stub.StreamObserver<org.apache.cassandra.compactlb.Coordination.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_REGISTER, responseObserver);
    }

    /**
     */
    public void watch(org.apache.cassandra.compactlb.Coordination.WatchReq request,
        io.grpc.stub.StreamObserver<org.apache.cassandra.compactlb.Coordination.WatchResp> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_WATCH, responseObserver);
    }

    /**
     */
    public void update(org.apache.cassandra.compactlb.Coordination.UpdateReq request,
        io.grpc.stub.StreamObserver<org.apache.cassandra.compactlb.Coordination.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_UPDATE, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_REGISTER,
            asyncUnaryCall(
              new MethodHandlers<
                org.apache.cassandra.compactlb.Coordination.RegisterReq,
                org.apache.cassandra.compactlb.Coordination.Empty>(
                  this, METHODID_REGISTER)))
          .addMethod(
            METHOD_WATCH,
            asyncServerStreamingCall(
              new MethodHandlers<
                org.apache.cassandra.compactlb.Coordination.WatchReq,
                org.apache.cassandra.compactlb.Coordination.WatchResp>(
                  this, METHODID_WATCH)))
          .addMethod(
            METHOD_UPDATE,
            asyncUnaryCall(
              new MethodHandlers<
                org.apache.cassandra.compactlb.Coordination.UpdateReq,
                org.apache.cassandra.compactlb.Coordination.Empty>(
                  this, METHODID_UPDATE)))
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
    public void register(org.apache.cassandra.compactlb.Coordination.RegisterReq request,
        io.grpc.stub.StreamObserver<org.apache.cassandra.compactlb.Coordination.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_REGISTER, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void watch(org.apache.cassandra.compactlb.Coordination.WatchReq request,
        io.grpc.stub.StreamObserver<org.apache.cassandra.compactlb.Coordination.WatchResp> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_WATCH, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void update(org.apache.cassandra.compactlb.Coordination.UpdateReq request,
        io.grpc.stub.StreamObserver<org.apache.cassandra.compactlb.Coordination.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_UPDATE, getCallOptions()), request, responseObserver);
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
    public org.apache.cassandra.compactlb.Coordination.Empty register(org.apache.cassandra.compactlb.Coordination.RegisterReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_REGISTER, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<org.apache.cassandra.compactlb.Coordination.WatchResp> watch(
        org.apache.cassandra.compactlb.Coordination.WatchReq request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_WATCH, getCallOptions(), request);
    }

    /**
     */
    public org.apache.cassandra.compactlb.Coordination.Empty update(org.apache.cassandra.compactlb.Coordination.UpdateReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_UPDATE, getCallOptions(), request);
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
    public com.google.common.util.concurrent.ListenableFuture<org.apache.cassandra.compactlb.Coordination.Empty> register(
        org.apache.cassandra.compactlb.Coordination.RegisterReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_REGISTER, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.cassandra.compactlb.Coordination.Empty> update(
        org.apache.cassandra.compactlb.Coordination.UpdateReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_UPDATE, getCallOptions()), request);
    }
  }

  private static final int METHODID_REGISTER = 0;
  private static final int METHODID_WATCH = 1;
  private static final int METHODID_UPDATE = 2;

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
        case METHODID_REGISTER:
          serviceImpl.register((org.apache.cassandra.compactlb.Coordination.RegisterReq) request,
              (io.grpc.stub.StreamObserver<org.apache.cassandra.compactlb.Coordination.Empty>) responseObserver);
          break;
        case METHODID_WATCH:
          serviceImpl.watch((org.apache.cassandra.compactlb.Coordination.WatchReq) request,
              (io.grpc.stub.StreamObserver<org.apache.cassandra.compactlb.Coordination.WatchResp>) responseObserver);
          break;
        case METHODID_UPDATE:
          serviceImpl.update((org.apache.cassandra.compactlb.Coordination.UpdateReq) request,
              (io.grpc.stub.StreamObserver<org.apache.cassandra.compactlb.Coordination.Empty>) responseObserver);
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
        METHOD_REGISTER,
        METHOD_WATCH,
        METHOD_UPDATE);
  }

}
