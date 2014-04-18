namespace java com.microsoft.corfu.sunit

include "common.thrift"

struct UnitWrap {
	1: common.CorfuErrorCode err,
	2: i32 lowwater,
	3: i32 highwater,
	4: i64 trimmark,
	5: i64 ckmark,
	6: list<common.LogPayload> ctnt,
	7: common.LogPayload bmap,
	}

service CorfuConfigServer {
    UnitWrap rebuild();
}