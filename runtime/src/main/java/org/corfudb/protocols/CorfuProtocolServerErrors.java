package org.corfudb.protocols;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.proto.Common.UuidMsg;
import org.corfudb.runtime.proto.ServerErrors.BootstrappedErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.DataCorruptionErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.DataOutrankedErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.NotBootstrappedErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.NotReadyErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.OverwriteErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.ServerErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.TrimmedErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.UnknownErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.ValueAdoptedErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.WrongClusterErrorMsg;
import org.corfudb.runtime.proto.ServerErrors.WrongEpochErrorMsg;

import java.io.IOException;
import java.io.ObjectOutputStream;

@Slf4j
public class CorfuProtocolServerErrors {
    public static ServerErrorMsg getWrongEpochErrorMsg(long correctEpoch) {
        return ServerErrorMsg.newBuilder()
                .setWrongEpochError(WrongEpochErrorMsg.newBuilder()
                        .setCorrectEpoch(correctEpoch)
                        .build())
                .build();
    }

    public static ServerErrorMsg getNotReadyErrorMsg() {
        return ServerErrorMsg.newBuilder()
                .setNotReadyError(NotReadyErrorMsg.getDefaultInstance())
                .build();
    }

    public static ServerErrorMsg getWrongClusterErrorMsg(UuidMsg expectedClusterId, UuidMsg providedClusterId) {
        return ServerErrorMsg.newBuilder()
                .setWrongClusterError(WrongClusterErrorMsg.newBuilder()
                        .setExpectedClusterId(expectedClusterId)
                        .setProvidedClusterId(providedClusterId)
                        .build())
                .build();
    }

    public static ServerErrorMsg getTrimmedErrorMsg() {
        return ServerErrorMsg.newBuilder()
                .setTrimmedError(TrimmedErrorMsg.getDefaultInstance())
                .build();
    }

    public static ServerErrorMsg getOverwriteErrorMsg(int causeId) {
        return ServerErrorMsg.newBuilder()
                .setOverwriteError(OverwriteErrorMsg.newBuilder()
                        .setOverwriteCauseId(causeId)
                        .build())
                .build();
    }

    public static ServerErrorMsg getDataOutrankedErrorMsg() {
        return ServerErrorMsg.newBuilder()
                .setDataOutrankedError(DataOutrankedErrorMsg.getDefaultInstance())
                .build();
    }

    //TODO(Zach): Complete after LogUnit implementation
    public static ServerErrorMsg getValueAdoptedErrorMsg() {
        return ServerErrorMsg.getDefaultInstance();
    }

    public static ServerErrorMsg getDataCorruptionErrorMsg(long address) {
        return ServerErrorMsg.newBuilder()
                .setDataCorruptionError(DataCorruptionErrorMsg.newBuilder()
                        .setAddress(address)
                        .build())
                .build();
    }

    public static ServerErrorMsg getBootstrappedErrorMsg() {
        return ServerErrorMsg.newBuilder()
                .setBootstrappedError(BootstrappedErrorMsg.getDefaultInstance())
                .build();
    }

    public static ServerErrorMsg getNotBootstrappedErrorMsg() {
        return ServerErrorMsg.newBuilder()
                .setNotBootstrappedError(NotBootstrappedErrorMsg.getDefaultInstance())
                .build();
    }

    //TODO(Zach): Complete me
    public static ServerErrorMsg getUnknownErrorMsg(Throwable throwable) {
        UnknownErrorMsg.Builder unknownErrorBuilder = UnknownErrorMsg.newBuilder();

        try(ByteString.Output bso = ByteString.newOutput()) {
            try(ObjectOutputStream oos = new ObjectOutputStream(bso)) {
                oos.writeObject(throwable);
                unknownErrorBuilder.setThrowable(bso.toByteString());
            }
        } catch (IOException ex) {


        }

        return ServerErrorMsg.newBuilder()
                .setUnknownError(unknownErrorBuilder.build())
                .build();
    }
}
