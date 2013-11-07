namespace java com.microsoft.corfu


enum CorfuErrorCode {
	OK,
	ERR_OVERWRITE,
	ERR_TRIMMED,
	ERR_UNWRITTEN,
	ERR_BADPARAM,
	ERR_FULL,
}

struct MetaInfo {
	1: i64 metaFirstOff,
	2: i64 metaLastOff,
}

struct LogHeader {
	1: MetaInfo range,
	2: bool readnext,
	3: i64 nextoff,
	4: CorfuErrorCode err,
	}
	
typedef binary LogPayload

struct LogEntryWrap {
	1: CorfuErrorCode err,
	2: MetaInfo nextinf,
	3: list<LogPayload> ctnt,
	}
	

