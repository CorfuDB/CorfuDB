namespace java com.microsoft.corfu.sunit	

include "common.thrift"

service CorfuUnitServer {

	common.CorfuErrorCode write(1:i64 fromoff, 2:list<common.LogPayload> ctnt, 3:common.MetaInfo inf),

	common.LogEntryWrap read(1:common.LogHeader hdr, 2:common.MetaInfo inf),
	
	common.LogEntryWrap readmeta(1:i64 off),

	i64 check(),
	
	i64 checkcontiguous(),
	
	bool trim (1:i64 mark),

}
