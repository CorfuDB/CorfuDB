namespace java com.microsoft.corfu


enum CorfuErrorCode {
	OK,
	ERR_OVERWRITE,
	ERR_TRIMMED,
	ERR_UNWRITTEN,
	ERR_BADPARAM,
	ERR_FULL,
}

struct LogHeader {
	1: i64 off,
	2: i32 ngrains,
	3: CorfuErrorCode err,
	}
	
typedef binary LogPayload

struct LogEntryWrap {
	1: LogHeader hdr,
	2: list<LogPayload> ctnt,
	}
	

