#!/usr/bin/env bash

if $cygwin
then
    KILL=/bin/kill
else
    KILL=kill
fi

for file in /var/run/corfudb*
do
	if [ -f $file ] 
	then
		echo killing $file
		if ! $KILL $(cat "$file")  
		then
			echo cannot kill $file
		else
			rm -f $file
		fi
	fi
done

find /var/tmp -name "corfudb*" -exec rm -f \{\} \;
find /var/log -name "corfudb*" -exec rm -f \{\} \;
