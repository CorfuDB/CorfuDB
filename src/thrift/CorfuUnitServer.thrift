namespace java com.microsoft.corfu.sunit	

include "common.thrift"

service CorfuUnitServer {

	common.CorfuErrorCode write(1:i64 position 2:common.LogPayload ctnt),

	common.CorfuPayloadWrap read(1:i64 offset),
	
	i64 check(),
	
	i64 checkcontiguous(),
	
	bool trim (1:i64 mark),

}
