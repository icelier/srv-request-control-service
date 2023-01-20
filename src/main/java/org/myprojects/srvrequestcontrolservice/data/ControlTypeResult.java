package org.myprojects.srvrequestcontrolservice.data;

import java.util.Objects;

public class ControlTypeResult {

    private Operators.ControlType controlType;
    private ServiceResponse.Status status;
    private String errorDescription;

    public ControlTypeResult(Operators.ControlType controlType, ServiceResponse.Status status,
                             String errorDescription) {
        this.controlType = controlType;
        this.status = status;
        this.errorDescription = errorDescription;
    }

    public ControlTypeResult(Operators.ControlType controlType) {
        this(controlType, ServiceResponse.Status.OK, null);
    }

    public Operators.ControlType getControlType() {
        return controlType;
    }

    public ServiceResponse.Status getStatus() {
        return status;
    }

    public String getErrorDescription() {
        return errorDescription;
    }

    public void setStatus(ServiceResponse.Status status) {
        this.status = status;
    }

    public void setErrorDescription(String errorDescription) {
        this.errorDescription = errorDescription;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ControlTypeResult that = (ControlTypeResult) o;
        return controlType == that.controlType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(controlType);
    }
}
