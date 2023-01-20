package org.myprojects.srvrequestcontrolservice.data;

public class ControlTypeRequest {

    private final Operators.ControlType controlType;
    private final ServiceRequest.DataFlowType dataFlowType;
    private final String filial;
    private Integer requestVersion;
    private String request;
    private IdList requestIdentifiers;

    private ControlTypeRequest(Operators.ControlType controlType, ServiceRequest.DataFlowType dataFlowType, String filial) {
        this.controlType = controlType;
        this.dataFlowType = dataFlowType;
        this.filial = filial;
    }

    public void setRequestVersion(Integer requestVersion) {
        this.requestVersion = requestVersion;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public void setRequestIdentifiers(IdList requestIdentifiers) {
        this.requestIdentifiers = requestIdentifiers;
    }

    public Operators.ControlType getControlType() {
        return controlType;
    }

    public ServiceRequest.DataFlowType getFlowType() {
        return dataFlowType;
    }

    public String getSegment() {
        return filial;
    }

    public Integer getRequestVersion() {
        return requestVersion;
    }

    public String getRequest() {
        return request;
    }

    public IdList getRequestIdentifiers() {
        return requestIdentifiers;
    }

    public static class Builder {

        private final ControlTypeRequest controlTypeRequest;

        public Builder(Operators.ControlType controlType, ServiceRequest.DataFlowType dataFlowType, String filial) {
            this.controlTypeRequest = new ControlTypeRequest(controlType, dataFlowType, filial);
        }

        public Builder setRequest(String request) {
            this.controlTypeRequest.setRequest(request);
            return this;
        }

        public Builder setRequestVersion(Integer requestVersion) {
            this.controlTypeRequest.setRequestVersion(requestVersion);
            return this;
        }

        public Builder setRequestIdentifiers(IdList requestIdentifiers) {
            this.controlTypeRequest.setRequestIdentifiers(requestIdentifiers);
            return this;
        }

        public ServiceRequest.DataFlowType getFlowType() {
            return this.controlTypeRequest.getFlowType();
        }

        public String getSegment() {
            return this.controlTypeRequest.getSegment();
        }

        public Integer getRequestVersion() {
            return this.controlTypeRequest.getRequestVersion();
        }

        public String getRequest() {
            return this.controlTypeRequest.getRequest();
        }

        public IdList getRequestIdentifiers() {
            return this.controlTypeRequest.getRequestIdentifiers();
        }

        public ControlTypeRequest build() {
            return this.controlTypeRequest;
        }
    }
}
