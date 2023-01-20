package org.myprojects.srvrequestcontrolservice.data;

import lombok.Getter;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Getter
public class ServiceRequest {

    @NotNull
    @NotBlank
    private final String filialName;
    @NotNull
    @NotBlank
    private final ServiceRequest.DataFlowType dataFlowType;
    @NotNull
    @NotBlank
    private String messageId;
    private Integer requestVersion;
    private IdList idList;
    private String checkRequestBody;
    @NotNull
    private final Operators operators;

    public ServiceRequest(DataFlowType dataFlowType, String filialName, String messageId, Operators operators) {
        this.filialName = filialName;
        this.dataFlowType = dataFlowType;
        this.messageId = messageId;
        this.operators = operators;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public void setRequestVersion(Integer requestVersion) {
        this.requestVersion = requestVersion;
    }

    public void setIdList(IdList idList) {
        this.idList = idList;
    }

    public void setCheckRequestBody(String checkRequestBody) {
        this.checkRequestBody = checkRequestBody;
    }

    public enum DataFlowType {
        DATA_FLOW_TYPE_1,
        DATA_FLOW_TYPE_2
    }
}
