namespace java com.microsoft.corfu.sunit	

include "common.thrift"

service CorfuUnitServer {

	common.CorfuErrorCode write(1:common.ExtntInfo inf, 2:list<common.LogPayload> ctnt),
	
	common.CorfuErrorCode fix(1:common.ExtntInfo inf),

	common.ExtntWrap read(1:common.CorfuHeader hdr),
	
	common.ExtntWrap readmeta(1:i64 off),

	i64 check(common.CorfuLogMark typ),
		
	bool trim (1:i64 mark),

}
