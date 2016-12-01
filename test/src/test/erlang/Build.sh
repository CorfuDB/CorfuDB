#!/bin/sh

case "$1" in
    eqc*)
        if [ ! -d $EQC_DIR/ebin ]; then
            echo "Env variable EQC_DIR is not set correctly, please set".
            exit 1
        fi
        export QC_EFLAGS="-DEQC -I ./include -pz ./ebin -pz $EQC_DIR/ebin -pz ./deps/pp_record/ebin"
        ;;
    proper*)
        if [ ! -d $PROPER_DIR/ebin ]; then
            echo "Env variable PROPER_DIR is not set correctly, please set".
            exit 1
        fi
        export QC_EFLAGS="-DPROPER -I ./include -pz ./ebin -pz $PROPER_DIR/ebin -pz ./deps/pp_record/ebin"
        ;;
    clean)
        exec make clean
        ;;
    *)
        echo "Usage: $0 eqc|eqc-shell|proper|proper-shell|clean"
        exit 1
        ;;
esac

case "$1" in
    *shell)
        shift
        set -x
        erl -sname ${QC_NODE_NAME:-qc} $QC_EFLAGS $*
        ;;
    *)
	if [ ! -d deps/pp_record/ebin ]; then
		rm -rf deps
		mkdir -p deps
		( cd deps ; git clone https://github.com/bet365/pp_record.git )
		( cd deps/pp_record ; make )
	fi
        exec make -j4 ebin
        ;;
esac
