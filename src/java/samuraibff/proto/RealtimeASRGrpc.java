package samuraibff.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class RealtimeASRGrpc {

  private RealtimeASRGrpc() {}

  public static final java.lang.String SERVICE_NAME = "RealtimeASR";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<samuraibff.proto.AudioChunk,
      samuraibff.proto.AsrEvent> getStreamMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Stream",
      requestType = samuraibff.proto.AudioChunk.class,
      responseType = samuraibff.proto.AsrEvent.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<samuraibff.proto.AudioChunk,
      samuraibff.proto.AsrEvent> getStreamMethod() {
    io.grpc.MethodDescriptor<samuraibff.proto.AudioChunk, samuraibff.proto.AsrEvent> getStreamMethod;
    if ((getStreamMethod = RealtimeASRGrpc.getStreamMethod) == null) {
      synchronized (RealtimeASRGrpc.class) {
        if ((getStreamMethod = RealtimeASRGrpc.getStreamMethod) == null) {
          RealtimeASRGrpc.getStreamMethod = getStreamMethod =
              io.grpc.MethodDescriptor.<samuraibff.proto.AudioChunk, samuraibff.proto.AsrEvent>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Stream"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  samuraibff.proto.AudioChunk.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  samuraibff.proto.AsrEvent.getDefaultInstance()))
              .setSchemaDescriptor(new RealtimeASRMethodDescriptorSupplier("Stream"))
              .build();
        }
      }
    }
    return getStreamMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RealtimeASRStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RealtimeASRStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RealtimeASRStub>() {
        @java.lang.Override
        public RealtimeASRStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RealtimeASRStub(channel, callOptions);
        }
      };
    return RealtimeASRStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static RealtimeASRBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RealtimeASRBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RealtimeASRBlockingV2Stub>() {
        @java.lang.Override
        public RealtimeASRBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RealtimeASRBlockingV2Stub(channel, callOptions);
        }
      };
    return RealtimeASRBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RealtimeASRBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RealtimeASRBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RealtimeASRBlockingStub>() {
        @java.lang.Override
        public RealtimeASRBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RealtimeASRBlockingStub(channel, callOptions);
        }
      };
    return RealtimeASRBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RealtimeASRFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RealtimeASRFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RealtimeASRFutureStub>() {
        @java.lang.Override
        public RealtimeASRFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RealtimeASRFutureStub(channel, callOptions);
        }
      };
    return RealtimeASRFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default io.grpc.stub.StreamObserver<samuraibff.proto.AudioChunk> stream(
        io.grpc.stub.StreamObserver<samuraibff.proto.AsrEvent> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getStreamMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service RealtimeASR.
   */
  public static abstract class RealtimeASRImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return RealtimeASRGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service RealtimeASR.
   */
  public static final class RealtimeASRStub
      extends io.grpc.stub.AbstractAsyncStub<RealtimeASRStub> {
    private RealtimeASRStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RealtimeASRStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RealtimeASRStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<samuraibff.proto.AudioChunk> stream(
        io.grpc.stub.StreamObserver<samuraibff.proto.AsrEvent> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getStreamMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service RealtimeASR.
   */
  public static final class RealtimeASRBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<RealtimeASRBlockingV2Stub> {
    private RealtimeASRBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RealtimeASRBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RealtimeASRBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<samuraibff.proto.AudioChunk, samuraibff.proto.AsrEvent>
        stream() {
      return io.grpc.stub.ClientCalls.blockingBidiStreamingCall(
          getChannel(), getStreamMethod(), getCallOptions());
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service RealtimeASR.
   */
  public static final class RealtimeASRBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<RealtimeASRBlockingStub> {
    private RealtimeASRBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RealtimeASRBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RealtimeASRBlockingStub(channel, callOptions);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service RealtimeASR.
   */
  public static final class RealtimeASRFutureStub
      extends io.grpc.stub.AbstractFutureStub<RealtimeASRFutureStub> {
    private RealtimeASRFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RealtimeASRFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RealtimeASRFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_STREAM = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_STREAM:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.stream(
              (io.grpc.stub.StreamObserver<samuraibff.proto.AsrEvent>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getStreamMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              samuraibff.proto.AudioChunk,
              samuraibff.proto.AsrEvent>(
                service, METHODID_STREAM)))
        .build();
  }

  private static abstract class RealtimeASRBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RealtimeASRBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return samuraibff.proto.StreamProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("RealtimeASR");
    }
  }

  private static final class RealtimeASRFileDescriptorSupplier
      extends RealtimeASRBaseDescriptorSupplier {
    RealtimeASRFileDescriptorSupplier() {}
  }

  private static final class RealtimeASRMethodDescriptorSupplier
      extends RealtimeASRBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    RealtimeASRMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (RealtimeASRGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RealtimeASRFileDescriptorSupplier())
              .addMethod(getStreamMethod())
              .build();
        }
      }
    }
    return result;
  }
}
