namespace java org.corfudb.infrastructure.thrift

include "Common.thrift"

struct SimpleLogUnitWrap {
	1: Common.ErrorCode err,
	2: i32 lowwater,
	3: i32 highwater,
	4: i64 trimmark,
	5: i64 ckmark,
	6: list<Common.LogPayload> ctnt,
	7: Common.LogPayload bmap,
	}

service SimpleLogUnitConfigService {
    void probe(),
    Common.ErrorCode phase2b(1:string config),
    string phase1b(1:i32 masterid),
    SimpleLogUnitWrap rebuild(),
	void kill(),
}
