#!/bin/sh

case "$1" in
    eqc*)
        if [ ! -d $EQC_DIR/ebin ]; then
            echo "Env variable EQC_DIR is not set correctly, please set".
            exit 1
        fi
        export QC_EFLAGS="-DEQC -I ./include -pz ./ebin -pz $EQC_DIR/ebin"
        ;;
    proper*)
        if [ ! -d $PROPER_DIR/ebin ]; then
            echo "Env variable PROPER_DIR is not set correctly, please set".
            exit 1
        fi
        export QC_EFLAGS="-DPROPER -I ./include -pz ./ebin -pz $PROPER_DIR/ebin"
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
        exec make -j4 ebin
        ;;
esac
