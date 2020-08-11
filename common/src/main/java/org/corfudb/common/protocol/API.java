package org.corfudb.common.protocol;

import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.corfudb.common.protocol.proto.CorfuProtocol.ERROR;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.MessageType;
import org.corfudb.common.protocol.proto.CorfuProtocol.PingRequest;
import org.corfudb.common.protocol.proto.CorfuProtocol.PingResponse;
import org.corfudb.common.protocol.proto.CorfuProtocol.Priority;
import org.corfudb.common.protocol.proto.CorfuProtocol.ProtocolVersion;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.ResetRequest;
import org.corfudb.common.protocol.proto.CorfuProtocol.ResetResponse;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;
import org.corfudb.common.protocol.proto.CorfuProtocol.RestartRequest;
import org.corfudb.common.protocol.proto.CorfuProtocol.RestartResponse;
import org.corfudb.common.protocol.proto.CorfuProtocol.SealRequest;
import org.corfudb.common.protocol.proto.CorfuProtocol.SealResponse;
import org.corfudb.common.protocol.proto.CorfuProtocol.ServerError;
import org.corfudb.common.protocol.proto.CorfuProtocol.WrongClusterPayload;

import java.util.UUID;

/**
 * Created by Maithem on 7/1/20.
 */
public class API {

    public static final ProtocolVersion CURRENT_VERSION = ProtocolVersion.v0;
    public static final UUID DEFAULT_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

    // Temporary message header markers indicating message type.
    public static final byte LEGACY_CORFU_MSG_MARK = 0x1;
    public static final byte PROTO_CORFU_MSG_MARK = 0x2;

    public static CorfuProtocol.UUID getUUID(UUID uuid) {
        return CorfuProtocol.UUID.newBuilder()
                .setLsb(uuid.getLeastSignificantBits())
                .setMsb(uuid.getMostSignificantBits())
                .build();
    }

    public static Header getHeader(long requestId, Priority priority, MessageType type, long epoch,
                                   CorfuProtocol.UUID clusterId, CorfuProtocol.UUID clientId,
                                   boolean ignoreClusterId, boolean ignoreEpoch) {
        return Header.newBuilder()
                .setVersion(CURRENT_VERSION)
                .setRequestId(requestId)
                .setPriority(priority)
                .setType(type)
                .setEpoch(epoch)
                .setClusterId(clusterId)
                .setClientId(clientId)
                .setIgnoreClusterId(ignoreClusterId)
                .setIgnoreEpoch(ignoreEpoch)
                .build();
    }

    public static Header getHeader(long requestId, Priority priority, MessageType type,
                                   long epoch, UUID clusterId, UUID clientId,
                                   boolean ignoreClusterId, boolean ignoreEpoch) {
        return getHeader(requestId, priority, type, epoch,
                getUUID(clusterId), getUUID(clientId), ignoreClusterId, ignoreEpoch);
    }

    public static Header generateResponseHeader(Header requestHeader, boolean ignoreClusterId, boolean ignoreEpoch) {
        return getHeader(requestHeader.getRequestId(),
                requestHeader.getPriority(),
                requestHeader.getType(),
                requestHeader.getEpoch(),
                requestHeader.getClusterId(),
                requestHeader.getClientId(),
                ignoreClusterId,
                ignoreEpoch);
    }

    public static ServerError getNoServerError() {
        return ServerError.newBuilder()
                .setCode(ERROR.OK)
                .setMessage("")
                .build();
    }

    public static ServerError getWrongEpochServerError(String errorMsg, long serverEpoch) {
        return ServerError.newBuilder()
                .setCode(ERROR.WRONG_EPOCH)
                .setMessage(errorMsg)
                .setWrongEpochPayload(serverEpoch)
                .build();
    }

    public static ServerError getNotReadyServerError(String errorMsg) {
        return ServerError.newBuilder()
                .setCode(ERROR.NOT_READY)
                .setMessage(errorMsg)
                .build();
    }

    public static ServerError getWrongClusterServerError(String errorMsg, CorfuProtocol.UUID serverClusterId,
                                                         CorfuProtocol.UUID clientClusterId) {
        return ServerError.newBuilder()
                .setCode(ERROR.WRONG_CLUSTER)
                .setMessage(errorMsg)
                .setWrongClusterPayload(WrongClusterPayload.newBuilder()
                        .setServerClusterId(serverClusterId)
                        .setClientClusterId(clientClusterId)
                        .build())
                .build();
    }

    public static ServerError getNotBootstrappedServerError(String errorMsg) {
        return ServerError.newBuilder()
                .setCode(ERROR.NOT_BOOTSTRAPPED)
                .setMessage(errorMsg)
                .build();
    }

    public static Request getPingRequest(Header header) {
        PingRequest pingRequest = PingRequest.getDefaultInstance();
        return Request.newBuilder()
                .setHeader(header)
                .setPingRequest(pingRequest)
                .build();
    }

    public static Response getPingResponse(Header header) {
        return getPingResponse(header, getNoServerError());
    }

    public static Response getPingResponse(Header header, ServerError error) {
        PingResponse pingResponse = PingResponse.getDefaultInstance();
        return Response.newBuilder()
                .setHeader(header)
                .setError(error)
                .setPingResponse(pingResponse)
                .build();
    }

    public static Request getRestartRequest(Header header) {
        RestartRequest restartRequest = RestartRequest.getDefaultInstance();
        return Request.newBuilder()
                .setHeader(header)
                .setRestartRequest(restartRequest)
                .build();
    }

    public static Response getRestartResponse(Header header) {
        return getRestartResponse(header, getNoServerError());
    }

    public static Response getRestartResponse(Header header, ServerError error) {
        RestartResponse restartResponse = RestartResponse.getDefaultInstance();
        return Response.newBuilder()
                .setHeader(header)
                .setError(error)
                .setRestartResponse(restartResponse)
                .build();
    }

    public static Request getResetRequest(Header header) {
        ResetRequest resetRequest = ResetRequest.getDefaultInstance();
        return Request.newBuilder()
                .setHeader(header)
                .setResetRequest(resetRequest)
                .build();
    }

    public static Response getResetResponse(Header header) {
        return getResetResponse(header, getNoServerError());
    }

    public static Response getResetResponse(Header header, ServerError error) {
        ResetResponse resetResponse = ResetResponse.getDefaultInstance();
        return Response.newBuilder()
                .setHeader(header)
                .setError(error)
                .setResetResponse(resetResponse)
                .build();
    }

    public static Request getSealRequest(Header header, long newEpoch) {
        SealRequest sealRequest = SealRequest.newBuilder()
                .setEpoch(newEpoch)
                .build();
        return Request.newBuilder()
                .setHeader(header)
                .setSealRequest(sealRequest)
                .build();
    }

    public static Response getSealResponse(Header header) {
        return getSealResponse(header, getNoServerError());
    }

    public static Response getSealResponse(Header header, ServerError error) {
        SealResponse sealResponse = SealResponse.getDefaultInstance();
        return Response.newBuilder()
                .setHeader(header)
                .setError(error)
                .setSealResponse(sealResponse)
                .build();
    }

    public static Response getErrorResponseNoPayload(Header header, ServerError error) {
        return Response.newBuilder()
                .setHeader(header)
                .setError(error)
                .build();
    }

    public static boolean validateRequestPayloadType(Request request) {
        switch(request.getHeader().getType()) {
            case PING:
                if (request.hasPingRequest()) return true;
                break;
            case AUTHENTICATE:
                if (request.hasAuthenticateRequest()) return true;
                break;
            case RESTART:
                if (request.hasRestartRequest()) return true;
                break;
            case RESET:
                if (request.hasResetRequest()) return true;
                break;
            case SEAL:
                if (request.hasSealRequest()) return true;
                break;
            case GET_LAYOUT:
                if (request.hasGetLayoutRequest()) return true;
                break;
            case PREPARE_LAYOUT:
                if (request.hasPrepareLayoutRequest()) return true;
                break;
            case PROPOSE_LAYOUT:
                if (request.hasProposeLayoutRequest()) return true;
                break;
            case COMMIT_LAYOUT:
                if (request.hasCommitLayoutRequest()) return true;
                break;
            case GET_TOKEN:
                if (request.hasGetTokenRequest()) return true;
                break;
            case COMMIT_TRANSACTION:
                if (request.hasCommitTransactionRequest()) return true;
                break;
            case BOOTSTRAP:
                if (request.hasBootstrapRequest()) return true;
                break;
            case QUERY_STREAM:
                if (request.hasQueryStreamRequest()) return true;
                break;
            case READ_LOG:
                if (request.hasReadLogRequest()) return true;
                break;
            case QUERY_LOG_METADATA:
                if (request.hasQueryLogMetadataRequest()) return true;
                break;
            case TRIM_LOG:
                if (request.hasTrimLogRequest()) return true;
                break;
            case COMPACT_LOG:
                if (request.hasCompactRequest()) return true;
                break;
            case FLASH:
                if (request.hasFlashRequest()) return true;
                break;
            case QUERY_NODE:
                if (request.hasQueryNodeRequest()) return true;
                break;
            case REPORT_FAILURE:
                if (request.hasReportFailureRequest()) return true;
                break;
            case HEAL_FAILURE:
                if (request.hasHealFailureRequest()) return true;
                break;
            case EXECUTE_WORKFLOW:
                if (request.hasExecuteWorkflowRequest()) return true;
                break;
            default:
                break;
        }

        return false;
    }
}
