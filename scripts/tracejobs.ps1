[console]::TreatControlCAsInput = $true

do { 
	get-job * | %{ Receive-Job $_; sleep 1;  }

          if ([console]::KeyAvailable) {
		$key = [system.console]::readkey($true)
		if (($key.modifiers -band [consolemodifiers]"control") -and ($key.key -eq "C")) {
			Add-Type -AssemblyName System.Windows.Forms
            		if ([System.Windows.Forms.MessageBox]::Show("Kill all background jobs?", "Exit Script?", 
			    [System.Windows.Forms.MessageBoxButtons]::YesNo) -eq "Yes")
		        {
				write-host -foregroundcolor White -backgroundcolor Red KILLING JOBS
				stop-job *
				rjb *
			}
			exit
		}
	  }

} while ($true)
