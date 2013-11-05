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
	1: i64 off,
	2: i32 ngrains,
	3: bool readnext,
	4: i64 nextoff,
	5: CorfuErrorCode err,
	}
	
typedef binary LogPayload

struct LogEntryWrap {
	1: CorfuErrorCode err,
	2: MetaInfo nextinf,
	3: list<LogPayload> ctnt,
	}
	

