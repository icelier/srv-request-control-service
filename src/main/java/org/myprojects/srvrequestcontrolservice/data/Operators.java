package org.myprojects.srvrequestcontrolservice.data;

import java.util.*;

public class Operators {

    private Map<ControlType, Operation> controlOperations;

    public Operators() {}

    public Operators(Map<ControlType, Operation> controlOperations) {
        this.controlOperations = controlOperations;
    }

    public Map<ControlType, Operation> getControlOperations() {
        return Collections.unmodifiableMap(controlOperations);
    }

    public Operation getCheckOperation(ControlType controlType) {
        return this.controlOperations.get(controlType);
    }

    public void setCheckOperation(ControlType controlType, Operation operation) {
        this.controlOperations.put(controlType, operation);
    }

    public boolean contains(ControlType controlType) {
        return controlOperations.containsKey(controlType);
    }

    public enum ControlType {
        REQUEST(0),
        CONTROL_IDENTIFIERS(1),
        REQUEST_VERSION(2),
        CLIENT_ATTRIBUTES(3);

        private int checkPriority;

        ControlType(int checkPriority) {
            this.checkPriority = checkPriority;
        }

        public int getCheckPriority() {
            return checkPriority;
        }
    }

    public enum Operation {
        UPDATE,
        CHECK,
        CHECK_AND_UPDATE,
        CACHE_CURRENT_VALUES,
        CONFIRM_REQUEST,
        RESTORE_FROM_CACHE
    }
 }
