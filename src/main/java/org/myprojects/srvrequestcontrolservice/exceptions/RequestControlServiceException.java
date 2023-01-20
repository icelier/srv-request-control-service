package org.myprojects.srvrequestcontrolservice.exceptions;

public class RequestControlServiceException extends RuntimeException {

    public RequestControlServiceException(String message) {
        super(message);
    }

    public RequestControlServiceException(String message, Throwable t) {
        super(message, t);
    }
}
