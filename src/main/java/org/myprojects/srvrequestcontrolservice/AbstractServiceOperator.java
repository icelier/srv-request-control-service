package org.myprojects.srvrequestcontrolservice;

import org.myprojects.srvrequestcontrolservice.data.ControlTypeRequest;
import org.myprojects.srvrequestcontrolservice.data.ControlTypeResult;
import org.myprojects.srvrequestcontrolservice.data.ServiceRequest;
import org.myprojects.srvrequestcontrolservice.exceptions.RequestControlServiceException;

import java.sql.Connection;

public abstract class AbstractServiceOperator {

    public abstract void updateServiceData(ControlTypeRequest controlTypeRequest, Connection conn)
            throws RequestControlServiceException;

    public abstract ControlTypeResult doServiceCheck(ControlTypeRequest controlTypeRequest, Connection conn);

    public abstract boolean validateRequest(ServiceRequest serviceRequest);
}
