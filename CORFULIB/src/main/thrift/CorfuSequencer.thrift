namespace java com.microsoft.corfu.sequencer


service CorfuSequencer {

	i64 nextpos(1:i32 ntokens);
	void recover(1:i64 lowbound);
		
}
