namespace java com.microsoft.corfu.sunit	

include "common.thrift"

service CorfuUnitServer {

	common.CorfuErrorCode write(1:common.MetaInfo inf, 2:list<common.LogPayload> ctnt),
	
	common.CorfuErrorCode fill(1:i64 pos),

	common.LogEntryWrap read(1:common.LogHeader hdr),
	
	common.LogEntryWrap readmeta(1:i64 off),

	i64 check(),
	
	i64 checkcontiguous(),
	
	bool trim (1:i64 mark),

}
