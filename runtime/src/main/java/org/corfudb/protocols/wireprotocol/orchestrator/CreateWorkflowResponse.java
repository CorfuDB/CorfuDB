package org.corfudb.protocols.wireprotocol.orchestrator;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorResponseType.WORKFLOW_CREATED;


import java.nio.ByteBuffer;
import java.util.UUID;
import lombok.Getter;

/**
 * CreateWorkflowResponse returns the UUID of a created workflow.
 *
 * @author Maithem
 */
public class CreateWorkflowResponse implements Response {

  @Getter public UUID workflowId;

  public CreateWorkflowResponse(UUID workflowId) {
    this.workflowId = workflowId;
  }

  public CreateWorkflowResponse(byte[] buf) {
    ByteBuffer bytes = ByteBuffer.wrap(buf);
    this.workflowId = new UUID(bytes.getLong(), bytes.getLong());
  }

  @Override
  public OrchestratorResponseType getType() {
    return WORKFLOW_CREATED;
  }

  @Override
  public byte[] getSerialized() {
    ByteBuffer buf = ByteBuffer.allocate(Long.BYTES * 2);
    buf.putLong(workflowId.getMostSignificantBits());
    buf.putLong(workflowId.getLeastSignificantBits());
    return buf.array();
  }
}
