namespace java org.corfudb.infrastructure.thrift


service SimpleSequencerService {

	i64 nextpos(1:i32 ntokens),
	void recover(1:i64 lowbound),
	void simulateFailure(1:bool fail, 2:i64 length),
    void reset(),
	bool ping();
}
