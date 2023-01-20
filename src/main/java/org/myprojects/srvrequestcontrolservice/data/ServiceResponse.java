package org.myprojects.srvrequestcontrolservice.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class ServiceResponse {

    @JsonProperty(required = true)
    private Status status;
    private String errorDescription;

    public ServiceResponse() {
        this.status = Status.OK;
    }

    public ServiceResponse(Status status) {
        this.status = status;
    }

    public enum Status {
        OK,
        ERROR,
        FAILED,
        OKWithWarnings
    }
}
