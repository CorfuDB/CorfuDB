namespace java org.corfudb.sequencer


service SequencerService {

	i64 nextpos(1:i32 ntokens);
	void recover(1:i64 lowbound);
		
}
