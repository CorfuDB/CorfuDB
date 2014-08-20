namespace java com.microsoft.corfu.loggingunit

include "Common.thrift"

struct LogUnitWrap {
	1: Common.ErrorCode err,
	2: i32 lowwater,
	3: i32 highwater,
	4: i64 trimmark,
	5: i64 ckmark,
	6: list<Common.LogPayload> ctnt,
	7: Common.LogPayload bmap,
	}

service LogUnitConfigService {
    void probe(),
    Common.ErrorCode phase2b(1:string config),
    string phase1b(1:i32 masterid),
    LogUnitWrap rebuild(),
	void kill(),
}