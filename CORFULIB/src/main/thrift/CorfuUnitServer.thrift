namespace java com.microsoft.corfu.sunit	

include "common.thrift"

service CorfuUnitServer {

	common.CorfuErrorCode write(1:i64 off, 2:list<common.LogPayload> ctnt, 3:common.ExtntMarkType et),
	
	common.CorfuErrorCode fix(1:i64 off),

	common.ExtntWrap read(1:i64 off),
	
	void sync(),
	
	common.ExtntWrap readmeta(1:i64 off),

	i64 querytrim(),
	
	i64 queryck(),
	
	void ckpoint(1:i64 off),
		
	bool trim (1:i64 mark),

}
