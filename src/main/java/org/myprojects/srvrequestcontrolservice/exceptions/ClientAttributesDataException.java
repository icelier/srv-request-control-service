package org.myprojects.srvrequestcontrolservice.exceptions;

public class ClientAttributesDataException extends RequestControlServiceException {

    public ClientAttributesDataException(String message) {
        super(message);
    }

    public ClientAttributesDataException(String message, Throwable t) {
        super(message, t);
    }
}
