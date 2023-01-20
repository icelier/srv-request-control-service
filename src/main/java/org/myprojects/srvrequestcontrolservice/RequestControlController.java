package org.myprojects.srvrequestcontrolservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.myprojects.srvrequestcontrolservice.data.ControlTypeRequest;
import org.myprojects.srvrequestcontrolservice.data.ServiceRequest;
import org.myprojects.srvrequestcontrolservice.data.ServiceResponse;
import org.myprojects.srvrequestcontrolservice.exceptions.RequestControlServiceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

@Slf4j
@RestController
@Validated
@RequestMapping("/requestControl")
public class RequestControlController {

    @Autowired
    private RequestControlService requestControlService;

    private final ObjectMapper mapper = new ObjectMapper();

    @PostMapping(value ="/control", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ServiceResponse process(@RequestBody @Valid ServiceRequest serviceRequest) throws JsonProcessingException {
        ServiceResponse serviceResponse;
        try {
            serviceResponse = requestControlService.processServiceRequest(serviceRequest);
        } catch (Exception e) {
            serviceResponse = new ServiceResponse(ServiceResponse.Status.FAILED,
                   String.format("Запрос с messageId %s не был обработан.%n%s",
                            serviceRequest.getMessageId(),
                           e.getMessage() == null ? "" : e.getMessage()));
        }

        log.info("ServiceResponse: " + mapper.writer().writeValueAsString(serviceResponse));
        return serviceResponse;
    }

    @PostMapping(value ="/clientAttributes/template/save", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ServiceResponse saveClientAttrsTemplate(@RequestBody ControlTypeRequest request) {
        try {
            LocalDateTime now = ZonedDateTime.now()
                    .withZoneSameInstant(ZoneId.systemDefault())
                    .toLocalDateTime();

            requestControlService.saveTemplate(request.getFlowType().name(), now,
                    request.getRequest());

            return new ServiceResponse();
        } catch (RequestControlServiceException e) {
            return new ServiceResponse(ServiceResponse.Status.FAILED,
                    e.getMessage() == null ? "" : e.getMessage());
        }
    }
}
