namespace java com.microsoft.corfu


enum CorfuErrorCode {
	OK,
	ERR_OVERWRITE,
	ERR_TRIMMED,
	ERR_UNWRITTEN,
	ERR_BADPARAM,
	ERR_FULL,
	OK_SKIP
}

enum CorfuLogMark {
	CONTIG,
	HEAD,
	TAIL
}

const i32 SKIPFLAG = 1

struct ExtntInfo {
	1: i64 metaFirstOff,
	2: i64 metaLastOff,
	3: i32 flag=false
}

struct CorfuHeader {
	1: ExtntInfo extntInf,
	2: bool prefetch,
	3: i64 prefetchOff,
	4: CorfuErrorCode err,
	}
	
typedef binary LogPayload

struct ExtntWrap {
	1: CorfuErrorCode err,
	2: ExtntInfo prefetchInf,
	3: list<LogPayload> ctnt,
	}
	

