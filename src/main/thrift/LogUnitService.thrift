namespace java  org.corfudb.infrastructure.thrift

include "Common.thrift"

service SimpleLogUnitService {

	Common.ErrorCode write(1:Common.UnitServerHdr hdr, 2:list<Common.LogPayload> ctnt, 3:Common.ExtntMarkType et),
	
	Common.ErrorCode fix(1:Common.UnitServerHdr hdr),

	Common.ExtntWrap read(1:Common.UnitServerHdr hdr),
	
	void sync(),
	
	Common.ExtntWrap readmeta(1:Common.UnitServerHdr hdr),

	i64 querytrim(),
	
	i64 queryck(),
	
	void ckpoint(1:Common.UnitServerHdr hdr),
		
    bool ping();
}
