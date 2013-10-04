namespace java com.microsoft.corfu.sunit	

include "common.thrift"

service CorfuUnitServer {

	common.CorfuErrorCode write(1:common.LogEntryWrap entry),

	common.LogEntryWrap read(1:common.LogHeader hdr),
	
	i64 check(),
	
	i64 checkcontiguous(),
	
	bool trim (1:i64 mark),

}
