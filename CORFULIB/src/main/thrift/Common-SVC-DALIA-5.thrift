namespace java org.corfudb


enum CorfuErrorCode {
	OK,
	ERR_OVERWRITE,
	ERR_TRIMMED,
	ERR_UNWRITTEN,
	ERR_BADPARAM,
	ERR_FULL,
	ERR_IO,
	OK_SKIP,
	ERR_STALEEPOCH,
}

enum ExtntMarkType {	EX_EMPTY, EX_FILLED, EX_TRIMMED, EX_SKIP }

struct ExtntInfo {
	1: i64 metaFirstOff,
	2: i32 metaLength,
	3: ExtntMarkType flag=ExtntMarkType.EX_FILLED
}

typedef binary LogPayload

struct ExtntWrap {
	1: CorfuErrorCode err,
	2: ExtntInfo inf,
	3: list<LogPayload> ctnt,
	}

struct UnitServerHdr {
    1: i64 epoch,
    2: i64 off,
}
	

