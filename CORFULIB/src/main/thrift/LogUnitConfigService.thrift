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
    void setConfig(1:string config),
    string getConfig(),
    LogUnitWrap rebuild(),
	Common.ErrorCode epochchange(1:Common.UnitServerHdr hdr),
	void kill(),
}