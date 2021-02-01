package org.corfudb.protocols;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import java.io.ObjectOutputStream;

import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.proto.ServerErrors.BootstrappedErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.DataCorruptionErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.NotBootstrappedErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.NotReadyErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.OverwriteErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.ServerErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.TrimmedErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.UnknownErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.WrongClusterErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.WrongEpochErrorMsg;

/**
 * This class provides methods for creating the Protobuf objects defined
 * in server_errors.proto and are used by several of the service RPCs.
 */
@Slf4j
public final class CorfuProtocolServerErrors {
    // Prevent class from being instantiated
    private CorfuProtocolServerErrors() {}

    /**
     * Returns the Protobuf representation of a WRONG_EPOCH error.
     *
     * @param correctEpoch   the correct epoch
     * @return               a ServerErrorMsg containing a WRONG_EPOCH error
     */
    public static ServerErrorMsg getWrongEpochErrorMsg(long correctEpoch) {
        return ServerErrorMsg.newBuilder()
                .setWrongEpochError(WrongEpochErrorMsg.newBuilder()
                        .setCorrectEpoch(correctEpoch)
                        .build())
                .build();
    }

    /**
     * Returns the Protobuf representation of a NOT_READY error.
     *
     * @return   a ServerErrorMsg containing a NOT_READY error
     */
    public static ServerErrorMsg getNotReadyErrorMsg() {
        return ServerErrorMsg.newBuilder()
                .setNotReadyError(NotReadyErrorMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns the Protobuf representation of a WRONG_CLUSTER_ID error.
     *
     * @param expectedClusterId   the cluster ID of the server
     * @param providedClusterId   the ID of the cluster provided by the client
     * @return                    a ServerErrorMsg containing a WRONG_CLUSTER_ID error
     */
    public static ServerErrorMsg getWrongClusterErrorMsg(UuidMsg expectedClusterId, UuidMsg providedClusterId) {
        return ServerErrorMsg.newBuilder()
                .setWrongClusterError(WrongClusterErrorMsg.newBuilder()
                        .setExpectedClusterId(expectedClusterId)
                        .setProvidedClusterId(providedClusterId)
                        .build())
                .build();
    }

    /**
     * Returns the Protobuf representation of a TRIMMED error.
     *
     * @return   a ServerErrorMsg containing a TRIMMED error
     */
    public static ServerErrorMsg getTrimmedErrorMsg() {
        return ServerErrorMsg.newBuilder()
                .setTrimmedError(TrimmedErrorMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns the Protobuf representation of a OVERWRITE error.
     *
     * @param causeId   the ID of the OverwriteCause in OverwriteException
     * @return          a ServerErrorMsg containing an OVERWRITE error
     */
    public static ServerErrorMsg getOverwriteErrorMsg(int causeId) {
        return ServerErrorMsg.newBuilder()
                .setOverwriteError(OverwriteErrorMsg.newBuilder()
                        .setOverwriteCauseId(causeId)
                        .build())
                .build();
    }

    /**
     * Returns the Protobuf representation of a DATA_CORRUPTION error.
     *
     * @param address   the address that caused the error
     * @return          a ServerErrorMsg containing a DATA_CORRUPTION error
     */
    public static ServerErrorMsg getDataCorruptionErrorMsg(long address) {
        return ServerErrorMsg.newBuilder()
                .setDataCorruptionError(DataCorruptionErrorMsg.newBuilder()
                        .setAddress(address)
                        .build())
                .build();
    }

    /**
     * Returns the Protobuf representation of a BOOTSTRAPPED error.
     *
     * @return   a ServerErrorMsg containing a BOOTSTRAPPED error
     */
    public static ServerErrorMsg getBootstrappedErrorMsg() {
        return ServerErrorMsg.newBuilder()
                .setBootstrappedError(BootstrappedErrorMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns the Protobuf representation of a NOT_BOOTSTRAPPED error.
     *
     * @return   a ServerErrorMsg containing a NOT_BOOTSTRAPPED error
     */
    public static ServerErrorMsg getNotBootstrappedErrorMsg() {
        return ServerErrorMsg.newBuilder()
                .setNotBootstrappedError(NotBootstrappedErrorMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns the Protobuf representation of an UNKNOWN_ERROR.
     * This is used for errors that do not fit with the above types.
     *
     * @param throwable   the underlying throwable cause
     * @return            a ServerErrorMsg containing an UNKNOWN_ERROR
     */
    public static ServerErrorMsg getUnknownErrorMsg(Throwable throwable) {
        UnknownErrorMsg.Builder unknownErrorBuilder = UnknownErrorMsg.newBuilder();

        try (ByteString.Output bso = ByteString.newOutput()) {
            try (ObjectOutputStream oos = new ObjectOutputStream(bso)) {
                oos.writeObject(throwable);
                unknownErrorBuilder.setThrowable(bso.toByteString());
            }
        } catch (Exception ex) {
            log.error("getUnknownErrorMsg: error while serializing throwable={}", throwable, ex);
        }

        return ServerErrorMsg.newBuilder()
                .setUnknownError(unknownErrorBuilder.build())
                .build();
    }
}
