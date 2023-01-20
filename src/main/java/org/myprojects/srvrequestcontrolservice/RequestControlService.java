package org.myprojects.srvrequestcontrolservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.myprojects.srvrequestcontrolservice.data.*;
import org.myprojects.srvrequestcontrolservice.exceptions.DatabaseException;
import org.myprojects.srvrequestcontrolservice.exceptions.RequestControlServiceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static org.myprojects.srvrequestcontrolservice.data.Operators.ControlType.*;
import static org.myprojects.srvrequestcontrolservice.data.Operators.Operation.CACHE_CURRENT_VALUES;
import static org.myprojects.srvrequestcontrolservice.data.Operators.Operation.CONFIRM_REQUEST;
import static org.myprojects.srvrequestcontrolservice.data.ServiceResponse.Status.*;

@Slf4j
@Service
@EnableScheduling
public class RequestControlService {

    @Value("${service.check.abort-on-check-error}")
    boolean abortOnCheckError;

    @Value("${service.check.client_attributes.enabled}")
    boolean clientAttributesCheckEnabled;

    @Value("${service.check.identifiers.enabled}")
    boolean identifiersCheckEnabled;

    @Value("${service.check.request_version.enabled}")
    boolean requestVersionCheckEnabled;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private ControlIdentifiersServiceOperator controlIdentifiersServiceOperator;
    @Autowired
    private RequestVersionServiceOperator requestVersionServiceOperator;
    @Autowired
    private ClientAttributesServiceOperator clientAttributesServiceOperator;

    private final ObjectMapper logMapper = new ObjectMapper();

    public ServiceResponse processServiceRequest(ServiceRequest serviceRequest) throws JsonProcessingException {

        log.info("Получен новый serviceRequest" + logMapper.writer().writeValueAsString(serviceRequest));

        // валидируем поступившие данные
        List<ControlTypeResult> validateResults = validateServiceRequest(serviceRequest);

        // проверяем есть ли ошибки валидации
        boolean validateResultsHasError = validateResults.stream()
                .anyMatch(r -> r.getStatus() != OK && r.getStatus() != OKWithWarnings);

        // если один из операторов не прошел валидацию, возвращаем ошибку
        if (validateResultsHasError) {
            return joinResults(validateResults);
        }

        // проверяем операторы уровня REQUEST (если есть)
        // (используется для обращений из Филиала, для подтверждения после получения ответа Мастер-системы проверок)
        if (serviceRequest.getOperators().contains(REQUEST)) {
            try {
                processRequestLevelOperations(serviceRequest);
            } catch (RequestControlServiceException e) {
                return new ServiceResponse(FAILED, String.format(
                        "Запрос с messageId %s не был обработан. %n%s", serviceRequest.getMessageId(),
                        e.getMessage() == null ? "" : e.getMessage()));
            }
        }

        // проверяем, если ли задачи на проверку данных
        List<ControlTypeResult> checkResults = doChecks(serviceRequest);
        boolean checkResultsHasError = checkResults.stream()
                .anyMatch(r -> r.getStatus() != OK && r.getStatus() != OKWithWarnings);
        // если проверки выполнились с ошибкой, возвращаем ошибку
        if (checkResultsHasError) {
            return joinResults(checkResults);
        }

        // отфильтровываем предупреждения для последующей отправки в ответе
        List<ControlTypeResult> checkWarnings = checkResults.stream()
                .filter(r -> r.getStatus() == OKWithWarnings)
                .collect(Collectors.toList());

        // выполняем задачи на обновление данных
        List<ControlTypeResult> updateResults;
        try {
            updateResults = doUpdates(serviceRequest);
        } catch (Exception e) {
            e.printStackTrace();
            return new ServiceResponse(FAILED, String.format("Запрос не был обработан. %n%s",
                    e.getMessage() == null ? "" : e.getMessage()));
        }
        boolean updateResultsHasError = updateResults.stream()
                .anyMatch(r -> r.getStatus() != OK);
        // если обновления выполнились с ошибкой, возвращаем ошибку
        if (updateResultsHasError) {
            // формируем общий ответ с учетом варнингов при проверках (если были)
            updateResults.addAll(checkWarnings);
            return joinResults(updateResults);
        }

        if (!checkWarnings.isEmpty()) {
            return joinResults(checkWarnings);
        }
        // возращаем статус ОК, если ошибок не было ни на одном этапе
        return new ServiceResponse();
    }

    private void processRequestLevelOperations(ServiceRequest serviceRequest)
            throws RequestControlServiceException {
        // помещаем в бд для кеша текущие (уже сохраненные в БД) значения ,
        // чтобы полностью подтвердить после успешного подтверждения от Мастер-системы проверок
        try (Connection conn = dataSource.getConnection()) {
            if (serviceRequest.getOperators().getCheckOperation(REQUEST)
                    == CACHE_CURRENT_VALUES) {
                processCacheRequest(serviceRequest, conn);
            }
            // восстанавливаем предыдущие значения из кеша
            else if (serviceRequest.getOperators().getCheckOperation(REQUEST)
                    == Operators.Operation.RESTORE_FROM_CACHE) {
                processRestoreRequest(serviceRequest, conn);
            }
            // подтверждаем ранее полученный запрос на сохранение данных в БД
            // в случае успешной обработки запроса внешней системой (Мастер-система, Главная система-проверок)
            else if (serviceRequest.getOperators().getCheckOperation(REQUEST)
                    == CONFIRM_REQUEST) {
                processConfirmRequest(serviceRequest, conn);
            }
        } catch (Exception e) {
            throw new RequestControlServiceException(e.getMessage());
        }
    }

    private void processCacheRequest(ServiceRequest serviceRequest, Connection conn) {
        // кешируем, если ранее уже передавались версии запроса
        if (serviceRequest.getRequestVersion() != 1) {
            try {
                // получаем текущие сохраненные в БД данные
                ServiceRequest currentValues = getCurrentValuesForCache(serviceRequest, conn);
                if (currentValues != null) {
                    cacheCurrentValues(currentValues, conn);
                } else {
                    // если в БД ничего не нашлось, проверяем если это первый запуск сервиса и в бд нет сохраненных данных
                    processServiceFirstRunForCache(serviceRequest, conn);
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RequestControlServiceException(String.format(
                        "Невозможно сохранить данные в кеш БД. %n%s",
                        e.getMessage() == null ? "" : e.getMessage()));
            }
        }
    }

    @Transactional
    public void processRestoreRequest(ServiceRequest serviceRequest, Connection conn) {
        try {
            ServiceRequest cachedData = getCachedData(serviceRequest, conn);
            if (cachedData != null) {
                restoreRequest(cachedData, conn);
                clearCachedData(serviceRequest, conn);
            } else {
                throw new RequestControlServiceException("Кешированные данные не найдены в базе данных");
            }
        } catch (RequestControlServiceException e) {
            throw new RequestControlServiceException(String.format(
                    "Невозможно выполнить восстановление ранее сохраненных данных из кеша. %n%s",
                    e.getMessage() == null ? "" : e.getMessage()));
        }
    }

    @Transactional
    public void processConfirmRequest(ServiceRequest serviceRequest, Connection conn) {
        try {
            ServiceRequest cachedData = getCachedData(serviceRequest, conn);
            if (cachedData != null) {
                clearCachedData(serviceRequest, conn);
                // выполняем обновление если необходимо, поскольку после подтверждения
                // могут быть добавлены новые идентификаторы
                ControlTypeRequest controlTypeRequest = generateControlTypeRequest(CONTROL_IDENTIFIERS, serviceRequest);
                controlIdentifiersServiceOperator.saveIdentifiersToDB(controlTypeRequest, conn);
            } else {
                throw new RequestControlServiceException("Кешированные данные не найдены в базе данных.");
            }
        } catch (RequestControlServiceException e) {
            throw new RequestControlServiceException(String.format(
                    "Невозможно выполнить подтверждение ранее полученного запроса. %n%s",
                    e.getMessage() == null ? "" : e.getMessage()));
        }
    }

    private void processServiceFirstRunForCache(ServiceRequest serviceRequest, Connection conn) {
        // идентификаторы могут быть пустые, если сервис только что установлен и в бд нет предыдущих данных
        // провеяем по идентификатору ID_INTEGRATION - если он есть, значит заявка уже ранее проходила интеграцию
        if (serviceRequest.getIdList() != null
                && serviceRequest.getIdList().get(RequestIdentifier.Id.ID_INTEGRATION) != null
                && !StringUtils.isBlank(serviceRequest.getIdList().
                get(RequestIdentifier.Id.ID_INTEGRATION).getIdValue())) {
            log.warn(String.format(
                    "Данные для запроса на кеширование с messageId %s не найдены в базе данных",
                    serviceRequest.getMessageId()));
            // сохраняем нуловые значения, чтобы отличить от реальной ошибки отсутствиия в кеш таблице
            ServiceRequest insuranceRequest = new ServiceRequest(
                    serviceRequest.getDataFlowType(),
                    serviceRequest.getFilialName(),
                    serviceRequest.getMessageId(),
                    new Operators());
            insuranceRequest.setIdList(new IdList());

            cacheCurrentValues(insuranceRequest, conn);
        } else {
            throw new RequestControlServiceException(String.format(
                    "Данные для запроса на кеширование с messageId %s не найдены в базе данных",
                    serviceRequest.getMessageId()));
        }
    }

    @Transactional
    public void restoreRequest(ServiceRequest cachedData, Connection conn) throws RequestControlServiceException {
        // удаляем сохраненные данные, которые не подтвердились с Мастер-системы (или Главной системы проверок)
        ControlTypeRequest controlTypeRequest = generateControlTypeRequest(REQUEST, cachedData);
        deleteFromRequestIdentifiers(controlTypeRequest, conn);
        // если кешированнеы данные не пустые, восстанавливаем их
        if (cachedData.getRequestVersion() != null && !cachedData.getIdList().getRequestIds().isEmpty()) {
            insertIdentifiersAndRequestVersionToDB(controlTypeRequest, conn);
        }
    }

    private ServiceRequest getCurrentValuesForCache(ServiceRequest serviceRequest, Connection conn)
            throws RequestControlServiceException {
        // создаем новый запрос под кеширвоанные данные
        ServiceRequest currentValues = new ServiceRequest(serviceRequest.getDataFlowType(), serviceRequest.getFilialName(),
                serviceRequest.getMessageId(), new Operators(serviceRequest.getOperators().getControlOperations()));

        // проверяем для каких операторов требуется кеширование
        for (Operators.ControlType controlType : serviceRequest.getOperators()
                .getControlOperations().keySet().stream()
                .sorted((Comparator.comparingInt(Operators.ControlType::getCheckPriority)))
                .collect(Collectors.toList())) {
            ControlTypeRequest controlTypeRequest = generateControlTypeRequest(controlType, serviceRequest);
            if (controlTypeRequest == null) {
                throw new IllegalArgumentException("Затребована неизвестная операция " + controlType.name());
            }
            if (controlType == CONTROL_IDENTIFIERS) {
                IdList idListFromDb = RequestControlService.getIdentifiersFromDB(controlTypeRequest, conn);
                if (!idListFromDb.getRequestIds().isEmpty()) {
                    currentValues.setIdList(idListFromDb);
                } else {
                    // возвращаем null, т.к. нет смысла кешировать нецелостные данные
                    return null;
                }
            } else if (controlType == REQUEST_VERSION) {
                Integer requestVersion = requestVersionServiceOperator.getRequestVersionFromDB(controlTypeRequest, conn);
                if (requestVersion != null) {
                    currentValues.setRequestVersion(requestVersion);
                } else {
                    // возвращаем null, т.к. нет смысла кешировать нецелостные данные
                    return null;
                }
            }
        }

        return currentValues;
    }

    @Transactional
    public void cacheCurrentValues(ServiceRequest currentValues, Connection conn)
            throws DatabaseException {
        String sql = "INSERT INTO request_cache (messageId, flow, filial, request_version, id_integration, id_master_system, id_filial, filial_id, id_main_check_system, request_type_id) VALUES(?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, currentValues.getMessageId());
            preparedStatement.setString(2, currentValues.getDataFlowType().name());
            preparedStatement.setString(3, currentValues.getFilialName());
            preparedStatement.setInt(4, currentValues.getRequestVersion());
            preparedStatement.setString(5, currentValues.getIdList().get(
                    RequestIdentifier.Id.ID_INTEGRATION) == null ? null : currentValues.getIdList().get(
                    RequestIdentifier.Id.ID_INTEGRATION).getIdValue());
            preparedStatement.setString(6, currentValues.getIdList().get(
                    RequestIdentifier.Id.ID_MASTER_SYSTEM) == null ? null : currentValues.getIdList().get(
                    RequestIdentifier.Id.ID_MASTER_SYSTEM).getIdValue());
            preparedStatement.setString(7, currentValues.getIdList().get(
                    RequestIdentifier.Id.ID_FILIAL) == null ? null : currentValues.getIdList().get(
                    RequestIdentifier.Id.ID_FILIAL).getIdValue());
            preparedStatement.setString(8, currentValues.getIdList().get(
                    RequestIdentifier.Id.FILIAL_ID) == null ? null : currentValues.getIdList().get(
                    RequestIdentifier.Id.FILIAL_ID).getIdValue());
            preparedStatement.setString(9, currentValues.getIdList().get(
                    RequestIdentifier.Id.ID_MAIN_CHECK_SYSTEM) == null ? null : currentValues.getIdList().get(
                    RequestIdentifier.Id.ID_MAIN_CHECK_SYSTEM).getIdValue());
            preparedStatement.setString(10, currentValues.getIdList().get(
                    RequestIdentifier.Id.REQUEST_TYPE_ID) == null ? null : currentValues.getIdList().get(
                    RequestIdentifier.Id.REQUEST_TYPE_ID).getIdValue());

            int updateCount = preparedStatement.executeUpdate();
            if (updateCount == 0) {
                throw new DatabaseException("Не удалось сохранить данные по идентификаторам и версии обращения в базу данных.");
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new DatabaseException(String.format("Не удалось сохранить данные по идентификаторам и версии обращения в базу данных:%n%s",
                    ex.getMessage() == null ? "" : ex.getMessage()));
        }
    }

    public ServiceRequest getCachedData(ServiceRequest currentServiceRequest, Connection conn)
            throws RequestControlServiceException {
        String sql = "SELECT message_id, request_version, id_integration, id_master_system, id_filial, id_main_check_system, filial_id, request_type_id FROM request_cache WHERE message_id=?";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, currentServiceRequest.getMessageId());
            ResultSet rs = preparedStatement.executeQuery();

            ServiceRequest cachedServiceRequest;
            Integer requestVersion = null;
            String messageId = null;
            List<RequestIdentifier> identifiers = new ArrayList<>();
            if (rs.next()) {
                messageId = rs.getString(1);
                requestVersion = rs.getInt(2);
                identifiers = RequestControlService.getIdentifiersFromResultSet(3, rs);
            }

            if (messageId != null) {
                cachedServiceRequest = new ServiceRequest(
                        currentServiceRequest.getDataFlowType(),
                        currentServiceRequest.getFilialName(),
                        currentServiceRequest.getMessageId(),
                        new Operators());
                cachedServiceRequest.setIdList(new IdList(identifiers));
                cachedServiceRequest.setRequestVersion(requestVersion);

                return cachedServiceRequest;
            } else {
                return null;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось получить кешированные данные из базы данных:%n%s",
                    ex.getMessage() == null ? "" : ex.getMessage()));
        }
    }

    @Transactional
    public void clearCachedData(ServiceRequest serviceRequest, Connection conn) {
        String sql = "DELETE FROM request_cache WHERE messageId=?";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, serviceRequest.getMessageId());

            int updateCount = preparedStatement.executeUpdate();
            if (updateCount == 0) {
                throw new DatabaseException("Данные для удаления из кеша базы данных не найдены.");
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new DatabaseException(String.format("Не удалось удалить данные в кеше базы данных:%n%s",
                    ex.getMessage() == null ? "" : ex.getMessage()));
        }
    }

    private List<ControlTypeResult> validateServiceRequest(ServiceRequest serviceRequest) {
        List<ControlTypeResult> validateResults = new ArrayList<>();

        List<Operators.ControlType> operators = serviceRequest.getOperators()
                .getControlOperations().keySet().stream()
                .sorted((Comparator.comparingInt(Operators.ControlType::getCheckPriority)))
                .collect(Collectors.toList());

        for (Operators.ControlType controlType : operators) {
            ControlTypeResult result;
            boolean validated = true;
            String errorDescription = "";
            switch (controlType) {
                case REQUEST:
                    errorDescription = validateRequestData(serviceRequest);
                    if (!errorDescription.isBlank()) {
                        errorDescription = "Недостаточно данных для обработки запроса. " + errorDescription;
                    }
                    break;
                case CONTROL_IDENTIFIERS:
                    // проверяем достаточность данных для проверки идентификаторов
                    if (identifiersCheckEnabled) {
                        validated = controlIdentifiersServiceOperator.validateRequest(serviceRequest);
                        if (!validated) {
                            errorDescription ="Недостаточно данных для обработки контрольных идентификаторов.";
                        }
                    }
                    break;
                case CLIENT_ATTRIBUTES:
                    // проверяем достаточность данных для проверки клиентских атрибутов
                    if (clientAttributesCheckEnabled) {
                        validated = clientAttributesServiceOperator.validateRequest(serviceRequest);
                        if (!validated) {
                            errorDescription = "Недостаточно данных для обработки клиентских атрибутов.";
                        }
                    }
                    break;
                case REQUEST_VERSION:
                    // проверяем достаточность данных для проверки версионности
                    if (requestVersionCheckEnabled) {
                        validated = requestVersionServiceOperator.validateRequest(serviceRequest);
                        if (!validated) {
                            errorDescription = "Недостаточно данных для обработки версионности.";
                        }
                    }
                    break;
                default:
                    throw new RequestControlServiceException("Неизвестный тип оператора " + controlType);
            }

            if (validated) {
                result = new ControlTypeResult(controlType);
            } else {
                result = new ControlTypeResult(controlType, FAILED, errorDescription);
            }

            // добавляем результат с ошибкой или со статусом ОК
            validateResults.add(result);
            if (controlType == REQUEST && result.getStatus() != OK) {
                return validateResults;
            }

        }

        return validateResults;
    }

    private String validateRequestData(ServiceRequest serviceRequest) {
        StringBuilder errorDescription = new StringBuilder();
        Operators.Operation requestOperation = serviceRequest.getOperators().getCheckOperation(REQUEST);

        if (requestOperation == CACHE_CURRENT_VALUES
                || requestOperation == CONFIRM_REQUEST) {
            if (serviceRequest.getIdList() == null ||
                    serviceRequest.getIdList().getRequestIds() == null ||
                    serviceRequest.getIdList().getRequestIds().isEmpty()) {
                errorDescription.append("В запросе не указаны идентификаторы обращения");
                errorDescription.append(";");
                errorDescription.append(System.lineSeparator());
            }
        }


        return errorDescription.toString();
    }

    public List<ControlTypeResult> doUpdates(ServiceRequest serviceRequest) throws DatabaseException {
        // отфильтровываем задачи на обновление
        List<Operators.ControlType> controlTypes = getUpdateControlTypes(serviceRequest.getOperators());

        try (Connection conn = dataSource.getConnection()) {
            return getUpdateResults(controlTypes, serviceRequest, conn);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось выполнить обновление обращения с messageId %s в базе данных. %n%s",
                    serviceRequest.getMessageId(),
                    e.getMessage() == null ? "" : e.getMessage()));
        }
    }

    private List<ControlTypeResult> getUpdateResults(List<Operators.ControlType> controlTypes,
                                                     ServiceRequest serviceRequest,
                                                     Connection conn)
            throws DatabaseException {

        List<ControlTypeResult> updateResults = new ArrayList<>();
        try {
            // если пришел запрос на обновление идентификаторов и версии запроса, сохраняем вместе
            if (controlTypes.contains(CONTROL_IDENTIFIERS)
                    && controlTypes.contains(REQUEST_VERSION)) {
                // генерируем запрос, т.к. он содержит все необходимые данные
                ControlTypeRequest controlTypeRequest = generateControlTypeRequest(Operators.ControlType.REQUEST,
                        serviceRequest);
                saveIdentifiersAndRequestVersionToDB(controlTypeRequest, conn);
                updateResults.add(new ControlTypeResult(REQUEST_VERSION));
                updateResults.add(new ControlTypeResult(CONTROL_IDENTIFIERS));
                controlTypes.remove(REQUEST_VERSION);
                controlTypes.remove(CONTROL_IDENTIFIERS);
            }

            // сохраняем для отдельных операторов
            for (Operators.ControlType controlType : controlTypes) {
                // генерируем запрос под конкретную задачу
                ControlTypeRequest controlTypeRequest = generateControlTypeRequest(controlType,
                        serviceRequest);
                if (controlTypeRequest == null) {
                    throw new RequestControlServiceException(String.format(
                            "Затребована неизвестная операция %s.", controlType.name()));
                }
                switch (controlType) {
                    case CONTROL_IDENTIFIERS:
                        controlIdentifiersServiceOperator.updateServiceData(controlTypeRequest, conn);
                        break;
                    case REQUEST_VERSION:
                        requestVersionServiceOperator.updateServiceData(controlTypeRequest, conn);
                        break;
                    case CLIENT_ATTRIBUTES:
                        clientAttributesServiceOperator.updateServiceData(controlTypeRequest, conn);
                        break;
                    default:
                        break;
                }
                // добавляем результат со статусом ОК
                updateResults.add(new ControlTypeResult(controlType));
            }
        } catch (RequestControlServiceException e) {
            // формируем результат с описанием ошибки и прекращаем дальнейшее обновление
            ControlTypeResult result = new ControlTypeResult(REQUEST, FAILED,
                    e.getMessage() == null ? "" : e.getMessage());
            updateResults.add(result);
            return updateResults;
        }

        return updateResults;
    }

    private List<ControlTypeResult> doChecks(ServiceRequest serviceRequest) {
        // отфильтровываем задачи на проверку
        // сортируем их по приоритетности
        List<Operators.ControlType> controlTypes = getCheckControlTypes(serviceRequest.getOperators());

        try (Connection conn = dataSource.getConnection()) {
            return getCheckResults(controlTypes, serviceRequest, conn);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось выполнить проверку обращения с messageId %s. %n%s",
                    serviceRequest.getMessageId(),
                    e.getMessage() == null ? "" : e.getMessage()));
        }
    }

    private List<ControlTypeResult> getCheckResults(List<Operators.ControlType> controlTypes,
                                                    ServiceRequest serviceRequest,
                                                    Connection conn) {
        List<ControlTypeResult> checkResults = new ArrayList<>();
        try {
            // если пришел запрос на проверку идентификаторов и версии запроса, проверяем вместе
            if (controlTypes.contains(CONTROL_IDENTIFIERS)
                    && controlTypes.contains(REQUEST_VERSION)
            && requestVersionCheckEnabled && identifiersCheckEnabled) {
                ControlTypeResult idsAndVersionResult = checkIdentifiersAndVersion(serviceRequest, conn);
                // добавляем результат проверки
                checkResults.add(idsAndVersionResult);
                controlTypes.remove(REQUEST_VERSION);
                controlTypes.remove(CONTROL_IDENTIFIERS);

                // если выставлен флаг не проводить последующие првоерки, если уже есть ошибка, возвращаем текущую ошибку
                if (abortOnCheckError && idsAndVersionResult.getStatus() == ServiceResponse.Status.ERROR) {
                    return checkResults;
                }
            }

            for (Operators.ControlType controlType : controlTypes) {
                ControlTypeResult result;
                // генерируем запрос для конкретной проверки
                ControlTypeRequest controlTypeRequest = generateControlTypeRequest(controlType, serviceRequest);
                if (controlTypeRequest == null) {
                    throw new IllegalArgumentException(String.format(
                            "Затребована неизвестная операция %s.", controlType.name()));
                }
                switch (controlType) {
                    case CONTROL_IDENTIFIERS:
                        if (identifiersCheckEnabled) {
                            result = controlIdentifiersServiceOperator.doServiceCheck(controlTypeRequest, conn);
                        } else {
                            result = new ControlTypeResult(controlType, OKWithWarnings,
                                    "Проверка идентификаторов отключена.");
                        }
                        break;
                    case REQUEST_VERSION:
                        if (requestVersionCheckEnabled) {
                            result = requestVersionServiceOperator.doServiceCheck(controlTypeRequest, conn);
                        } else {
                            result = new ControlTypeResult(controlType, OKWithWarnings,
                                    "Проверка версии обращения отключена.");
                        }
                        break;
                    case CLIENT_ATTRIBUTES:
                        if (clientAttributesCheckEnabled) {
                            result = clientAttributesServiceOperator.doServiceCheck(controlTypeRequest, conn);
                        } else {
                            result = new ControlTypeResult(controlType, OKWithWarnings,
                                    "Проверка клиентских атрибутов отключена.");
                        }
                        break;
                    default:
                        throw new RequestControlServiceException("Неизвестный тип оператора " + controlType);
                }
                checkResults.add(result);
                // если выставлен флаг не проводить последующие проверки, если уже есть ошибка, возвращаем текущую ошибку
                if (abortOnCheckError && result.getStatus() == ServiceResponse.Status.ERROR) {
                    break;
                }
            }

            return checkResults;
        } catch (RequestControlServiceException e) {
            // формируем результат с описанием ошибки
            List<ControlTypeResult> results = new ArrayList<>();
            results.add(new ControlTypeResult(REQUEST, FAILED, String.format(
                    "Не удалось выпоонить проверку обращения. %n%s",
                    e.getMessage() == null ? "" : e.getMessage())));
            return results;
        }
    }

    private List<Operators.ControlType> getCheckControlTypes(Operators operators) {
        return operators
                .getControlOperations().entrySet().stream()
                .filter(e -> e.getValue() == Operators.Operation.CHECK
                        || e.getValue() == Operators.Operation.CHECK_AND_UPDATE)
                .map(Map.Entry::getKey)
                .sorted((Comparator.comparingInt(Operators.ControlType::getCheckPriority)))
                .collect(Collectors.toList());
    }

    private List<Operators.ControlType> getUpdateControlTypes(Operators operators) {
        return operators
                .getControlOperations().entrySet().stream()
                .filter(e -> e.getValue() == Operators.Operation.UPDATE
                        || e.getValue() == Operators.Operation.CHECK_AND_UPDATE)
                .map(Map.Entry::getKey)
                .sorted((Comparator.comparingInt(Operators.ControlType::getCheckPriority)))
                .collect(Collectors.toList());
    }

    private ControlTypeResult checkIdentifiersAndVersion(ServiceRequest serviceRequest,
                                                         Connection conn) {
        // генерируем запрос для полного запроса
        ControlTypeRequest controlTypeRequest = generateControlTypeRequest(REQUEST, serviceRequest);
        List<String> errors = new ArrayList<>();
        ControlTypeResult result = new ControlTypeResult(REQUEST);
        try {
            IdList identifiersFromDB = RequestControlService.getIdentifiersFromDB(controlTypeRequest, conn);
            if (identifiersFromDB.getRequestIds().isEmpty()) {
                // если у обращения версия 1 или это первый запуск сервиса и сохраненных в БД данных еще нет
                // возвращаем статус ОК
                if (controlTypeRequest.getRequestVersion() == 1
                        || (controlTypeRequest.getRequestIdentifiers().
                        get(RequestIdentifier.Id.ID_INTEGRATION) != null
                        && !StringUtils.isBlank(controlTypeRequest.getRequestIdentifiers().
                        getValue(RequestIdentifier.Id.ID_INTEGRATION)))) {
                    return new ControlTypeResult(REQUEST);
                } else {
                    // в противном случае обращние должно бытьс версией 1
                    return new ControlTypeResult(REQUEST, ServiceResponse.Status.ERROR,
                            "Номер версии создаваемого обращения должен быть равен 1");
                }
            }

            Integer savedRequestVersion = requestVersionServiceOperator.getRequestVersionFromDB(controlTypeRequest, conn);
            if (savedRequestVersion == null) {
                return new ControlTypeResult(controlTypeRequest.getControlType(), FAILED,
                        "Не найден номер версии обращения");
            }

            // если пришла старая версия заявки, возвращаем ошибку
            if (savedRequestVersion >= controlTypeRequest.getRequestVersion()) {
                result.setStatus(ServiceResponse.Status.ERROR);
                errors.add(String.format("Версия передаваемого обращения устарела. " +
                                "Пожалуйста, получите актуальную версию '%d' перед обновлением",
                        savedRequestVersion));
            }

            // если не включен флаг завершить првоерки при первой ошибке или если включен, но ошибок нет
            // продолжаем проверку идентфиикаторов
            if (!abortOnCheckError || result.getStatus() != ERROR) {
                errors.addAll(controlIdentifiersServiceOperator.checkIdentifiers(
                        identifiersFromDB, controlTypeRequest, conn));
            }

            return errors.isEmpty() ? new ControlTypeResult(REQUEST)
                    : new ControlTypeResult(REQUEST, ServiceResponse.Status.ERROR,
                    String.join(";" + System.lineSeparator(), errors));
        } catch (RequestControlServiceException e) {
            return new ControlTypeResult(REQUEST, FAILED,
                    e.getMessage() == null ? "" : e.getMessage());
        }
    }

    private ControlTypeRequest generateControlTypeRequest(Operators.ControlType controlType,
                                                          ServiceRequest serviceRequest) {
        ControlTypeRequest controlTypeRequest;
        switch (controlType) {
            case REQUEST:
                // формируем запрос для проверки только идентификаторов
                controlTypeRequest = new ControlTypeRequest.Builder(controlType, serviceRequest.getDataFlowType(),
                        serviceRequest.getFilialName())
                        .setRequestVersion(serviceRequest.getRequestVersion())
                        .setRequestIdentifiers(serviceRequest.getIdList())
                        .setRequest(serviceRequest.getCheckRequestBody())
                        .build();
                break;
            case REQUEST_VERSION:
                // формируем запрос для проверки только идентификаторов
                controlTypeRequest = new ControlTypeRequest.Builder(controlType, serviceRequest.getDataFlowType(),
                        serviceRequest.getFilialName())
                        .setRequestVersion(serviceRequest.getRequestVersion())
                        .setRequestIdentifiers(serviceRequest.getIdList())
                        .build();
                break;
            case CONTROL_IDENTIFIERS:
                // формируем запрос для проверки только идентификаторов
                controlTypeRequest = new ControlTypeRequest.Builder(controlType, serviceRequest.getDataFlowType(),
                        serviceRequest.getFilialName())
                        .setRequestIdentifiers(serviceRequest.getIdList())
                        .build();
                break;
            case CLIENT_ATTRIBUTES:
                // формируем запрос для проверки только клиентских атрибутов
                controlTypeRequest = new ControlTypeRequest.Builder(controlType, serviceRequest.getDataFlowType(),
                        serviceRequest.getFilialName())
                        .setRequestIdentifiers(new IdList(List.of(
                                serviceRequest.getIdList().get(RequestIdentifier.Id.ID_MASTER_SYSTEM))))
                        .setRequest(serviceRequest.getCheckRequestBody())
                        .build();
                break;
            default:
                throw new RequestControlServiceException("Неизвестный тип оператора " + controlType);
        }

        return controlTypeRequest;
    }

    private ServiceResponse joinResults(List<ControlTypeResult> results) {
        ServiceResponse serviceResponse = new ServiceResponse();

        StringBuilder errorDescription = new StringBuilder();
        // проверяем были ли проблемы обработки
        for (ControlTypeResult result : results) {
            if (result.getStatus() == FAILED) {
                errorDescription.append(result.getErrorDescription());
                errorDescription.append(";");
                errorDescription.append(System.lineSeparator());
            }
        }
        if (!errorDescription.toString().isEmpty()) {
            // возвращаем результат с ошибкой обработки запроса т.к.
            // без полной проверки непонятно как интерпретировать результат на принимающей стороне
            serviceResponse.setStatus(FAILED);
            serviceResponse.setErrorDescription(errorDescription.toString());
            return serviceResponse;
        }

        // проверяем были ли ошибки при проверках
        for (ControlTypeResult result : results) {
            if (result.getStatus() != OK) {
                errorDescription.append(result.getErrorDescription());
                errorDescription.append(";");
                errorDescription.append(System.lineSeparator());
                if (serviceResponse.getStatus() == OK
                        || serviceResponse.getStatus() == OKWithWarnings) {
                    serviceResponse.setStatus(result.getStatus());
                }
            }
        }
        if (!errorDescription.toString().isEmpty()) {
            serviceResponse.setErrorDescription(errorDescription.toString());
        }

        return serviceResponse;
    }

    @Transactional
    public void deleteFromRequestIdentifiers(ControlTypeRequest serviceRequest, Connection conn)
            throws RequestControlServiceException {
        PreparedStatement preparedStatement = null;
        try {
            String sql;
            int updateCount = 0;
            // сначала ищем по идентификатору ID_MASTER_SYSTEM
            if (serviceRequest.getRequestIdentifiers().get(RequestIdentifier.Id.ID_MASTER_SYSTEM) != null
                    && !StringUtils.isBlank(serviceRequest.getRequestIdentifiers().get(RequestIdentifier.Id.ID_MASTER_SYSTEM).getIdValue())
                    && serviceRequest.getRequestIdentifiers().get(RequestIdentifier.Id.ID_INTEGRATION) != null
                    && !StringUtils.isBlank(serviceRequest.getRequestIdentifiers().get(RequestIdentifier.Id.ID_INTEGRATION).getIdValue())) {
                sql = "DELETE FROM request_identifiers WHERE flow=? AND filial=? AND id_master_system=? AND id_integration=?";
                preparedStatement = conn.prepareStatement(sql);
                RequestControlService.setPreparedStatementStringParams(preparedStatement,
                        serviceRequest.getFlowType().name(),
                        serviceRequest.getSegment(),
                        serviceRequest.getRequestIdentifiers().get(RequestIdentifier.Id.ID_MASTER_SYSTEM).getIdValue(),
                        serviceRequest.getRequestIdentifiers().get(RequestIdentifier.Id.ID_INTEGRATION).getIdValue());

                updateCount = preparedStatement.executeUpdate();
            }
            // если его нет в обращении,  ищем по идентификатору ID_FILIAL
            if (updateCount == 0 && serviceRequest.getRequestIdentifiers().get(RequestIdentifier.Id.ID_FILIAL) != null
                    && !StringUtils.isBlank(serviceRequest.getRequestIdentifiers().get(RequestIdentifier.Id.ID_FILIAL).getIdValue())
                    && serviceRequest.getRequestIdentifiers().get(RequestIdentifier.Id.ID_INTEGRATION) != null
                    && !StringUtils.isBlank(serviceRequest.getRequestIdentifiers().get(RequestIdentifier.Id.ID_INTEGRATION).getIdValue())) {
                sql = "DELETE FROM request_identifiers WHERE flow=? AND filial=? AND id_filial=? AND id_integration=?";
                preparedStatement = conn.prepareStatement(sql);
                RequestControlService.setPreparedStatementStringParams(preparedStatement,
                        serviceRequest.getFlowType().name(),
                        serviceRequest.getSegment(),
                        serviceRequest.getRequestIdentifiers().get(RequestIdentifier.Id.ID_FILIAL).getIdValue(),
                        serviceRequest.getRequestIdentifiers().get(RequestIdentifier.Id.ID_INTEGRATION).getIdValue());

                updateCount = preparedStatement.executeUpdate();
            }
            if (updateCount == 0) {
                throw new RequestControlServiceException("Недостаточно данных для удаления данных из базы данных");
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new DatabaseException(String.format("Не удалось удалить данные в базе данных:%n%s",
                    ex.getMessage() == null ? "" : ex.getMessage()));
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    log.warn("Не удалось закрыть preparedStatement должным образом.");
                }
            }
        }
    }

    public void saveIdentifiersAndRequestVersionToDB(ControlTypeRequest controlTypeRequest, Connection conn)
            throws DatabaseException {
        if (controlTypeRequest.getRequestVersion() == 1) {
            // сохраняем первую версию заявки
            insertIdentifiersAndRequestVersionToDB(controlTypeRequest, conn);
        } else {
            IdList identifiersFromDb = RequestControlService.getIdentifiersFromDB(controlTypeRequest, conn);

            if (identifiersFromDb.getRequestIds().isEmpty()) {
                // сохраняем заявку, поступившую впервые (при установке сервиса)
                insertIdentifiersAndRequestVersionToDB(controlTypeRequest, conn);
            } else {
                // обновляем заявку, если она поступила не впервые
                updateVersionAndMissingIdentifiers(controlTypeRequest, identifiersFromDb, conn);
            }
        }
    }

    @Transactional
    public void insertIdentifiersAndRequestVersionToDB(ControlTypeRequest controlTypeRequest, Connection conn) throws DatabaseException {
        String sql = "INSERT INTO request_identifiers (flow, filial, request_version, id_integration, id_master_system, id_filial, filial_id, id_main, request_type_id) VALUES(?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, controlTypeRequest.getFlowType().name());
            preparedStatement.setString(2, controlTypeRequest.getSegment());
            preparedStatement.setInt(3, controlTypeRequest.getRequestVersion());
            preparedStatement.setString(4, controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_INTEGRATION) == null ? null : controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_INTEGRATION).getIdValue());
            preparedStatement.setString(5, controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_MASTER_SYSTEM) == null ? null : controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_MASTER_SYSTEM).getIdValue());
            preparedStatement.setString(6, controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_FILIAL) == null ? null : controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_FILIAL).getIdValue());
            preparedStatement.setString(7, controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.FILIAL_ID) == null ? null : controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.FILIAL_ID).getIdValue());
            preparedStatement.setString(8, controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_MAIN_CHECK_SYSTEM) == null ? null : controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_MAIN_CHECK_SYSTEM).getIdValue());
            preparedStatement.setString(9, controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.REQUEST_TYPE_ID) == null ? null : controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.REQUEST_TYPE_ID).getIdValue());

            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось сохранить данные по идентификаторам и версии обращения в базу данных:%n%s",
                    ex.getMessage() == null ? "" : ex.getMessage()));
        }
    }

    @Transactional
    public void updateVersionAndMissingIdentifiers(ControlTypeRequest controlTypeRequest,
                                                    IdList identifiersFromDb, Connection conn) throws DatabaseException {
        IdList requestIdentifiers = controlTypeRequest.getRequestIdentifiers();

        List<String> params = new ArrayList<>();
        // отбираем уже сохраненные в бд идентификаторы
        List<RequestIdentifier> notEmptyIdentifiersFromDb = identifiersFromDb.getRequestIds().stream()
                .filter(id -> id != null && id.getIdValue() != null && !id.getIdValue().isEmpty())
                .collect(Collectors.toList());
        // проверяем, какие идентификаторы не были ранее сохранены в бд
        List<RequestIdentifier> emptyIdentifiersFromDb = identifiersFromDb.getRequestIds().stream()
                .filter(id -> id.getIdValue() == null || id.getIdValue().isEmpty())
                .filter(id -> requestIdentifiers.contains(id.getIdName()))
                .collect(Collectors.toList());
        StringBuilder sqlBuilder = new StringBuilder("UPDATE request_identifiers SET request_version=?");
        for (RequestIdentifier id : emptyIdentifiersFromDb) {
            if (requestIdentifiers.get(id.getIdName()) != null) {
                sqlBuilder.append(", ");
                sqlBuilder.append(id.getIdName()).append("=?");
                params.add(requestIdentifiers.get(id.getIdName()).getIdValue());
            }
        }

        sqlBuilder.append(" WHERE flow=? AND filial=?");
        params.add(controlTypeRequest.getFlowType().name());
        params.add(controlTypeRequest.getSegment());

        for (RequestIdentifier id : notEmptyIdentifiersFromDb) {
            sqlBuilder.append(" AND ");
            sqlBuilder.append(id.getIdName()).append("=?");
            params.add(id.getIdValue());
        }

        try (PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString())) {
            ps.setInt(1, controlTypeRequest.getRequestVersion());
            for (int i = 1; i < params.size() + 1; i++) {
                ps.setString(i + 1, params.get(i - 1));
            }

            int updateCount = ps.executeUpdate();
            if (updateCount == 0) {
                throw new DatabaseException("Не удалось обновить данные по идентификаторам и версии обращения в базе данных.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось обновить данные по идентификаторам и версии обращения в базе данных:%n%s",
                    e.getMessage() == null ? "" : e.getMessage()));
        }
    }

    public static void setPreparedStatementStringParams(PreparedStatement preparedStatement,
                                                                     int index,
                                                                     String... params)
            throws SQLException {
        for (int i = 0, length = params.length; i < length; i++) {
            preparedStatement.setString(++index, params[i]);
        }
    }

    public static void setPreparedStatementStringParams(PreparedStatement preparedStatement,
                                                                     String... params)
            throws SQLException {
        setPreparedStatementStringParams(preparedStatement, 0, params);
    }

    public void saveTemplate(String flow, LocalDateTime now, String request) throws DatabaseException {
        try (Connection conn = dataSource.getConnection()) {
            clientAttributesServiceOperator.saveTemplate(flow, now, request, conn);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RequestControlServiceException(String.format(
                    "Не удалось сохранить шаблон для клиентских атрибутов в базе данных. %n%s",
                    e.getMessage() == null ? "" : e.getMessage()));
        }
    }

    public static IdList getIdentifiersFromDB(ControlTypeRequest controlTypeRequest, Connection conn)
            throws DatabaseException {
        PreparedStatement preparedStatement = null;
        List<RequestIdentifier> identifiers = new ArrayList<>();
        try {
            String sql;
            // сначала пытаемся запросить по id Мастер-системы
            if (controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_MASTER_SYSTEM) != null
                    && !StringUtils.isBlank(controlTypeRequest.getRequestIdentifiers().
                    get(RequestIdentifier.Id.ID_MASTER_SYSTEM).getIdValue())
                    && controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_INTEGRATION) != null
                    && !StringUtils.isBlank(controlTypeRequest.getRequestIdentifiers().
                    get(RequestIdentifier.Id.ID_INTEGRATION).getIdValue())) {
                sql = "SELECT id_integration, id_master_system, id_filial, id_main, filial_id, request_type_id  FROM request_identifiers WHERE flow=? AND filial=? AND id_master_system=? AND id_integration=?";
                preparedStatement = conn.prepareStatement(sql);
                RequestControlService.setPreparedStatementStringParams(
                        preparedStatement,
                        controlTypeRequest.getFlowType().name(),
                        controlTypeRequest.getSegment(),
                        controlTypeRequest.getRequestIdentifiers().get(
                                RequestIdentifier.Id.ID_MASTER_SYSTEM).getIdValue(),
                        controlTypeRequest.getRequestIdentifiers().get(
                                RequestIdentifier.Id.ID_INTEGRATION).getIdValue());
                identifiers = RequestControlService.getIdentifiersFromResultSet(1, preparedStatement.executeQuery());
            }
            // если id Мастер-системы не было в запросе (например, для заявок Филиала) или по нему ничего не нашлось, ищем по id Филиала
            if (identifiers.isEmpty() && controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_FILIAL) != null
                    && !StringUtils.isBlank(controlTypeRequest.getRequestIdentifiers().
                    get(RequestIdentifier.Id.ID_FILIAL).getIdValue())
                    && controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_INTEGRATION) != null
                    && !StringUtils.isBlank(controlTypeRequest.getRequestIdentifiers().
                    get(RequestIdentifier.Id.ID_INTEGRATION).getIdValue())) {
                sql = "SELECT id_integration, id_master_system, id_filial, id_main, filial_id, request_type_id FROM request_identifiers WHERE flow=? AND filial=? AND id_filial=? AND id_integration=?";
                preparedStatement = conn.prepareStatement(sql);
                RequestControlService.setPreparedStatementStringParams(preparedStatement,
                        controlTypeRequest.getFlowType().name(),
                        controlTypeRequest.getSegment(),
                        controlTypeRequest.getRequestIdentifiers().get(
                                RequestIdentifier.Id.ID_FILIAL).getIdValue(),
                        controlTypeRequest.getRequestIdentifiers().get(
                                RequestIdentifier.Id.ID_INTEGRATION).getIdValue());
                identifiers = RequestControlService.getIdentifiersFromResultSet(1, preparedStatement.executeQuery());
            }
            return new IdList(identifiers);
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось получить данные из базы данных:%n%s",
                    ex.getMessage() == null ? "" : ex.getMessage()));
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    log.warn("PreparedStatement не был закрыт должным образом");
                }
            }
        }
    }

    public static List<RequestIdentifier> getIdentifiersFromResultSet(int index, ResultSet rs) throws SQLException {
        List<RequestIdentifier> identifiers = new ArrayList<>();
        if (rs.next()) {
            identifiers.add(new RequestIdentifier(RequestIdentifier.Id.ID_INTEGRATION, rs.getString(index++)));
            identifiers.add(new RequestIdentifier(RequestIdentifier.Id.ID_MASTER_SYSTEM, rs.getString(index++)));
            identifiers.add(new RequestIdentifier(RequestIdentifier.Id.ID_FILIAL, rs.getString(index++)));
            identifiers.add(new RequestIdentifier(RequestIdentifier.Id.ID_MAIN_CHECK_SYSTEM, rs.getString(index++)));
            identifiers.add(new RequestIdentifier(RequestIdentifier.Id.FILIAL_ID, rs.getString(index++)));
            identifiers.add(new RequestIdentifier(RequestIdentifier.Id.REQUEST_TYPE_ID, rs.getString(index++)));
        }

        return identifiers;
    }
}
