package com.capiot.streambase.mongoUtil;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.operator.AlreadyRegisteredException;
import com.streambase.sb.operator.Operator;

/**
 * Created by abilashmohan on 03/10/16.
 */
public class SharedMongoClientManager implements Operator.SharedObjectManager {

    @Override
    public void registerSharedObject(Object o, Operator.SharedObject sharedObject) throws AlreadyRegisteredException, StreamBaseException {

    }

    @Override
    public Operator.SharedObject getSharedObject(Object o) {
        return null;
    }

    @Override
    public Operator.SharedObject removeSharedObject(Object o) {
        return null;
    }

    @Override
    public Operator.SharedObject.State getSharedObjectState(Object o) {
        return null;
    }
}
