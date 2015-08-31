namespace java  org.corfudb.infrastructure.thrift

include "Common.thrift"

enum ReadCode { READ_OK, READ_EMPTY, READ_FILLEDHOLE, READ_TRIMMED }
enum HintType { TXN }

struct Hint
{
    1:HintType hint,
    2:binary data
}

struct ReadResult
{
   1:ReadCode code,
   2:set<Common.UUID> stream,
   3:binary data,
   4:set<Hint> hints
}

service NewLogUnitService {

	Common.WriteResult write(1:i64 epoch, 2:i64 offset, 3: set<Common.UUID> stream, 4:binary payload),
	ReadResult read(1:i64 epoch, 2:i64 offset),
	Common.ErrorCode trim(1:i64 epoch, 2: Common.UUID stream, 3:i64 prefix),

    bool ping(),
    void reset()
}
