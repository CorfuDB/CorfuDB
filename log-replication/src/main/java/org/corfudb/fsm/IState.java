package org.corfudb.fsm;

public interface IState {

    void enter();

    void exit();
}
