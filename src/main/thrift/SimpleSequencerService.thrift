namespace java org.corfudb.infrastructure.thrift


service SimpleSequencerService {

	i64 nextpos(1:i32 ntokens);
	void recover(1:i64 lowbound);
	bool ping();
}
