namespace java com.microsoft.corfu

typedef binary LogPayload

enum CorfuErrorCode {
	OK,
	ERR_OVERWRITE,
	ERR_TRIMMED,
	ERR_UNWRITTEN,
	ERR_BADPARAM,
	ERR_FULL,
}


struct CorfuOffsetWrap {
	1: CorfuErrorCode offerr,
	2: i64 off,
}

struct CorfuPayloadWrap {
	1: CorfuErrorCode err,
	2: LogPayload ctnt,
}



