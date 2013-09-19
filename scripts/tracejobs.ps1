do { 
	get-job * | %{ Receive-Job $_; sleep 1; } 
} while ($true)
