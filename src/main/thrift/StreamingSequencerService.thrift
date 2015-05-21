namespace java org.corfudb.infrastructure.thrift

struct StreamSequence {
   1:i64 position,
   2:i32 totalTokens
}

service StreamingSequencerService {

	i64 nextpos(1:i32 ntokens);
    StreamSequence nextstreampos(1:string streamID, 2:i32 ntokens);
    void setAllocationSize(1: string streamID, 2:i32 size);
	void recover(1:i64 lowbound);
	void simulateFailure(1:bool fail, 2:i64 length),
    void reset();
	bool ping();
}
