package org.myprojects.srvrequestcontrolservice;

import lombok.extern.slf4j.Slf4j;
import org.myprojects.srvrequestcontrolservice.data.*;
import org.myprojects.srvrequestcontrolservice.exceptions.ClientAttributesDataException;
import org.myprojects.srvrequestcontrolservice.exceptions.DatabaseException;
import org.myprojects.srvrequestcontrolservice.exceptions.RequestControlServiceException;
import org.myprojects.srvrequestcontrolservice.utils.SimpleCache;
import org.myprojects.srvrequestcontrolservice.utils.TempCache;
import org.myprojects.srvrequestcontrolservice.utils.XmlUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.sql.DataSource;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static org.myprojects.srvrequestcontrolservice.data.RequestIdentifier.Id.ID_MASTER_SYSTEM;
import static org.myprojects.srvrequestcontrolservice.utils.XmlUtils.HASH;

@Slf4j
@EnableScheduling
@Component
public class ClientAttributesServiceOperator extends AbstractServiceOperator {

    public static class ClientAttributesConstants {

        private ClientAttributesConstants() {}

        public static final String CLIENT_ATTRIBUTE = "clientAttribute";
        public static final String PATH = "path";
        public static final String PERSONAL_DATA = "personalData";
        public static final String ERROR_DESCRIPTION = "errorDescription";
        public static final String PREFIX_ERROR_DESCRIPTION = "prefixDescription";
        public static final String POSTFIX_ERROR_DESCRIPTION = "postfixDescription";
    }

    @Autowired
    private DataSource dataSource;

    @Autowired
    // кэш шаблонов
    private SimpleCache<XmlRequestTemplate> templateCache;
    @Autowired
    // кэш распарсенных сохраненных запросов
    private TempCache<TempCache.Unit<ParsedXmlRequest>> savedRequestsCache;

    @Override
    public void updateServiceData(ControlTypeRequest controlTypeRequest, Connection conn)
            throws RequestControlServiceException {
        saveRequest(controlTypeRequest, conn);
    }

    @Override
    public ControlTypeResult doServiceCheck(ControlTypeRequest controlTypeRequest, Connection conn) {
        return checkClientAttributes(controlTypeRequest, conn);
    }

    @Override
    public boolean validateRequest(ServiceRequest serviceRequest) {
        return serviceRequest.getIdList() != null
                && serviceRequest.getIdList().getRequestIds() != null
                && !serviceRequest.getIdList().getRequestIds().isEmpty()
                && serviceRequest.getIdList().contains(ID_MASTER_SYSTEM)
                && serviceRequest.getIdList().get(ID_MASTER_SYSTEM).getIdValue() != null
                && !serviceRequest.getIdList().get(ID_MASTER_SYSTEM).getIdValue().isBlank()
                && serviceRequest.getCheckRequestBody() != null
                && !serviceRequest.getCheckRequestBody().isBlank();
    }

    public ControlTypeResult checkClientAttributes(ControlTypeRequest controlTypeRequest, Connection conn) {
        Operators.ControlType controlType = controlTypeRequest.getControlType();

        LocalDateTime now = ZonedDateTime.now()
                .withZoneSameInstant(ZoneId.systemDefault())
                .toLocalDateTime();

        try {
            // получаем актуальный сохраненный шаблон клиентских атрибутов
            XmlRequestTemplate template = getCurrentTemplate(controlTypeRequest.getFlowType().name(), now, conn);

            // получаем дерево входящего запроса
            Document checkRequestDoc = XmlUtils.getDocumentFromXmlString(controlTypeRequest.getRequest());


            // получаем актуальный документ последнего сохраненного запроса
            XmlRequestPaths effectiveRequest = getEffectiveRequest(
                    controlTypeRequest.getFlowType().name(),
                    controlTypeRequest.getSegment(),
                    controlTypeRequest.getRequestIdentifiers().getRequestIds().get(0).getIdValue(),
                    template,
                    conn);

            // если запрос не найден в БД, возвращаем соответствующий статус
            if (effectiveRequest == null) {
                return new ControlTypeResult(controlType, ServiceResponse.Status.FAILED,
                        String.format("Невозможно выполнить проверку клиентских атрибутов. Сохраненное обращение с идентификатором %s не найдено",
                                controlTypeRequest.getRequestIdentifiers().getRequestIds().get(0).getIdValue()));
            }

            // получаем дерево проверяемого запроса
            Map<XmlPath, Node> checkRequestPaths = matchClientAttributesByPaths(template, checkRequestDoc, true);
            ParsedXmlRequest parsedCheckRequest = new ParsedXmlRequest(controlTypeRequest.getFlowType().name(),
                    controlTypeRequest.getSegment(),
                    controlTypeRequest.getRequestIdentifiers().getRequestIds().get(0).getIdValue(),
                    checkRequestPaths);

            // проверяем, что значения клиентских атрибутов в сохраненном и проверяемом запросах не изменились
            List<String> errorDescription = checkClientAttributesMatch(template, effectiveRequest, parsedCheckRequest);

            // если нет ошибок, возвращаем статус ОК
            if (errorDescription.isEmpty()) {
                return new ControlTypeResult(controlType);
            }

            List<String> updatedDescription = new ArrayList<>(new HashSet<>(errorDescription));
            updatedDescription.sort(String::compareTo);

            // возвращаем описание ошибок
            return new ControlTypeResult(controlType, ServiceResponse.Status.ERROR,
                    String.join(";" + System.lineSeparator(), updatedDescription));
        } catch (RequestControlServiceException e) {
            // если словили ошибку, возвращаем описание ошибки
            return new ControlTypeResult(controlType, ServiceResponse.Status.FAILED,
                    e.getMessage() == null ? "" : e.getMessage());
        } catch (ParserConfigurationException | IOException | SAXException e) {
            return new ControlTypeResult(controlType, ServiceResponse.Status.FAILED,
                    "Не удалось обработать xml структуру запроса на проверку клиентских атрибутов.");
        }
    }

    private XmlRequestTemplate getCurrentTemplate(String flow, LocalDateTime timestamp, Connection conn)
            throws ClientAttributesDataException {
        // получаем из базы название последнего актуального шаблона на дату
        String effectiveTemplateName = getEffectiveTemplateName(flow, timestamp, conn);

        if (effectiveTemplateName == null) {
            throw new ClientAttributesDataException(String.format(
                    "Не найдены данные о шаблоне по клиентским атрибутам для потока %s", flow));
        }

        // проверяем, был ли шаблон под таким названием сохранен в кэше
        XmlRequestTemplate effectiveTemplate = templateCache.getCachedUnit(effectiveTemplateName);
        if (effectiveTemplate == null ) {
            // удаляем предыдущие шаблоны из кеша
            List<String> allEffectiveTemplateNames = getAllEffectiveTemplateNames(conn);
            templateCache.clearCachedUnitExcept(allEffectiveTemplateNames);

            // загружаем новый шаблон из базы и сохраняем в кэш
            effectiveTemplate = getEffectiveTemplate(flow, timestamp, conn);

            if (effectiveTemplate == null) {
                throw new ClientAttributesDataException(String.format(
                        "Не найдены данные о шаблоне по клиентским атрибутам для потока %s", flow));
            }
            // сохраняем новый шаблон в кеш
            templateCache.cacheUnit(effectiveTemplateName, effectiveTemplate);
        }

        return effectiveTemplate;
    }

    private XmlRequestPaths getEffectiveRequest(String flow, String filial, String messageId,
                                                XmlRequestTemplate template,
                                                Connection conn)
            throws DatabaseException, ClientAttributesDataException {
        TempCache.Unit<ParsedXmlRequest> cacheData = savedRequestsCache.getCachedUnit(
                getRequestIdentifier(flow, filial, messageId));

        LocalDateTime lastUpdate = getSavedRequestLastUpdate(flow, filial, messageId, conn);

        if (lastUpdate == null) {
            return null;
        }
        ParsedXmlRequest effectiveRequest = null;
        // проверяем в кэше сохраненный запрос с распарсенными путями и сравниваем с последнием обновлением в БД
        if (cacheData != null) {
            effectiveRequest = cacheData.getCacheUnit();

            LocalDateTime currentUpdate = effectiveRequest.getLastUpdated();
            if (!currentUpdate.isEqual(lastUpdate)) {
                effectiveRequest = null;
            }
        }
        if (effectiveRequest == null) {
            // получаем актуальный сохраненный запрос из БД
            String requestStr = getSavedRequest(flow, filial, messageId, conn);

            if (requestStr == null) {
                throw new ClientAttributesDataException(
                        String.format("Не найдены данные по клиентским атрибутам для потока %s", flow));
            }
            Document requestDoc;
            try {
                requestDoc = XmlUtils.getDocumentFromXmlString(requestStr);
            } catch (ParserConfigurationException | IOException | SAXException e) {
                e.printStackTrace();
                throw new IllegalArgumentException("Не удалось обработать xml структуру запроса.");
            }
            Map<XmlPath, Node> effectiveRequestPaths = matchClientAttributesByPaths(template, requestDoc, false);
            effectiveRequest = new ParsedXmlRequest(flow, filial, messageId, lastUpdate, effectiveRequestPaths);
            savedRequestsCache.cacheUnit(
                    getRequestIdentifier(flow, filial, messageId),
                    new TempCache.Unit<>(effectiveRequest));
        }

        return effectiveRequest;
    }

    private List<String> checkClientAttributesMatch(XmlRequestPaths template,
                                                    XmlRequestPaths savedRequest,
                                                    XmlRequestPaths checkRequest) throws ClientAttributesDataException {
        Map<XmlPath, Node> templatePaths = template.getPaths();
        Map<XmlPath, Node> savedRequestPaths = savedRequest.getPaths();
        Map<XmlPath, Node> checkRequestPaths = checkRequest.getPaths();

        // перебираем все найденные пути в сохраненном и проверяемом запросах и сравниваем, изменились ли значения клиенстких атрибутов
        List<String> errors = getCheckRequestNonMatchErrors(savedRequestPaths, checkRequestPaths, templatePaths);

        // находим клиентские атрибуты, которых не было в сохраненном запросе, но появились в проверяемом и добавляем ошибки
        Map<XmlPath, Node> checkReqAdditionalPaths = checkRequestPaths.entrySet().stream()
                .filter(e -> !savedRequestPaths.containsKey(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        List<String> additionalErrors = getCheckRequestAdditionalErrors(checkReqAdditionalPaths, templatePaths);
        // объединяем ошибки
        errors.addAll(additionalErrors);

        return errors;
    }

    private static List<String> getCheckRequestNonMatchErrors(Map<XmlPath, Node> savedRequestPaths,
                                                              Map<XmlPath, Node> checkRequestPaths,
                                                              Map<XmlPath, Node> templatePaths)
            throws ClientAttributesDataException {
        List<String> errors = new ArrayList<>();
        XmlPath currentPath;
        for (Map.Entry<XmlPath, Node> savedReqPath : savedRequestPaths.entrySet()) {
            currentPath = savedReqPath.getKey();
            XmlPath templatePath = currentPath;
            // если путь динамический (т е в пути есть ноды с произвольным значением (например, sectionNumber для секций))
            // формируем динамический путь для шаблона, соответствующий динамическому пути запроса с конкретным динамическим значением
            boolean pathIsDynamic = XmlUtils.isDynamicPath(currentPath);
            if (pathIsDynamic) {
                templatePath = XmlUtils.createTemplateDynamicPath(currentPath);
            }
            Node templatePathNode = templatePaths.get(templatePath);

            if (templatePathNode == null) {
                throw new IllegalArgumentException("Обнаружена неизвестная структура данных.");
            }
            // ищем соответствующий путь в проверяемом запросе
            Node checkReqPathNode = checkRequestPaths.get(currentPath);

            Node savedReqPathNode = savedReqPath.getValue();
            boolean match;
            String requestValue = "";
            String savedValue = "";
            if (savedReqPathNode.getFirstChild() == null || savedReqPathNode.getFirstChild().getNodeValue() == null
                    || savedReqPathNode.getFirstChild().getNodeValue().isBlank()) {
                savedValue = "null";
                if (checkReqPathNode == null || checkReqPathNode.getFirstChild() == null
                        || checkReqPathNode.getFirstChild().getNodeValue() == null
                        || checkReqPathNode.getFirstChild().getNodeValue().isBlank()) {
                    match = true;
                    requestValue = "null";
                }
                else {
                    match = false;
                    requestValue = checkReqPathNode.getFirstChild().getNodeValue();
                }

            } else {
                // если в сохраненном запросе клиентский атрибут передан с текстовым значением
                // то в проверяемом запросе клиентский атрибут должен либо отсутствовать,
                // либо значение клиентского атрибута не должно меняться по сравнению с сохраненным значение
                savedValue = savedReqPathNode.getFirstChild().getNodeValue();
                if (checkReqPathNode == null || checkReqPathNode.getFirstChild() == null
                        || checkReqPathNode.getFirstChild().getNodeValue() == null
                        || checkReqPathNode.getFirstChild().getNodeValue().isBlank()) {
                    match = true;
                    requestValue = "checkReqPathNode is null or empty and doesn't change client attribute";
                } else {
                    // сравниваем сохраненное и проверяемое значения
                    requestValue = checkReqPathNode.getFirstChild().getNodeValue();
                    // проверяем файлы
                    requestValue = XmlUtils.checkFileAttribute(requestValue);
                    savedValue = XmlUtils.checkFileAttribute(savedValue);

                    match = XmlUtils.compareClientAttrsValues(savedValue, requestValue);
                }
            }

            if (!match) {
                log.warn("Attributes don't match.\nPath: {}\nRequest value: {}\n  Saved value: {}",
                        currentPath,
                        requestValue,
                        savedValue);
            }

            // если значение клиентского атрибута поменялось в проверяемом запросе,
            // добавляем ошибку клиентского атрибута
            if (!match) {
                String error = getErrorDescription(pathIsDynamic, templatePath, templatePathNode, savedReqPathNode);
                if (savedReqPathNode.getFirstChild() == null) {
                    error = error + ". Ранее указанный атрибут не передавался.";
                } else {
                    if (!XmlUtils.nodeHasPersonalDataMark(templatePathNode)
                            && !savedValue.contains(HASH)) {
                        error = error + ". Ранее передаваемое значение атрибута - "
                                + savedReqPathNode.getFirstChild().getNodeValue();
                    }
                }
                errors.add(error);
            }
        }

        return errors;
    }

    private static List<String> getCheckRequestAdditionalErrors(Map<XmlPath, Node> checkReqAdditionalPaths,
                                                                Map<XmlPath, Node> templatePaths)
            throws ClientAttributesDataException {
        List<String> errors = new ArrayList<>();
        XmlPath currentPath;
        for (Map.Entry<XmlPath, Node> path : checkReqAdditionalPaths.entrySet()) {
            currentPath = path.getKey();
            Node pathNode = path.getValue();
            if (!XmlUtils.nodeIsTextValueNode(pathNode)) {
                continue;
            }
            // если атрибута раньше не было, а сейчас пришел 0, то ошибки нет
            if (XmlUtils.checkIsZeroNumberAttribute(pathNode)) {
                continue;
            }
            XmlPath templatePath = currentPath;
            boolean pathIsDynamic = XmlUtils.isDynamicPath(currentPath);
            if (pathIsDynamic) {
                templatePath = XmlUtils.createTemplateDynamicPath(currentPath);
            }
            Node templatePathNode = templatePaths.get(templatePath);

            if (templatePathNode == null) {
                throw new IllegalArgumentException("Обнаружена неизвестная структура данных.");
            }


            String error = getErrorDescription(pathIsDynamic, templatePath, templatePathNode, pathNode);
            errors.add(error  + ". Ранее указанный атрибут не передавался.");
        }

        return errors;
    }

    private static String getErrorDescription(boolean pathIsDynamic, XmlPath templatePath, Node templatePathNode,
                                              Node pathNode) throws ClientAttributesDataException {
        String error = null;
        if (pathIsDynamic) {
            return XmlUtils.getDynamicErrorDescription(templatePath, templatePathNode, pathNode);
        } else {
            error = XmlUtils.getErrorDescription(templatePathNode);
            if (error == null) {
                throw new ClientAttributesDataException("Не найдено описание ошибки атрибута "
                        + templatePathNode.getNodeName());
            }

            return error;
        }
    }

    private Map<XmlPath, Node> matchClientAttributesByPaths(XmlRequestPaths templatePaths,
                                                            Document requestDocument,
                                                            boolean hashPersData) {
        Map<XmlPath, Node> clientAttributes = new HashMap<>();
        XmlPath lastSavedPath = null;
        // проходим в дереве запроса по сохраненным путям шаблона, по которым находятся ноды с клиентскими атрибутамм
        for (Map.Entry<XmlPath, Node> entry : templatePaths.getPaths().entrySet()) {
            // получаем пути к тегам с клиентским атрибутом
            Map<XmlPath, Node> requestPaths;

            if (lastSavedPath == null || XmlUtils.isDynamicPath(lastSavedPath) || XmlUtils.isDynamicPath(entry.getKey())) {
                // ищем с начала дерева, если пути еще не проверялись или в случае динамического пути
                requestPaths = XmlUtils.findNodesMatchingPathFromParent(requestDocument, entry.getKey());
            }
            // если ранее пути проверялись
            // проверяем, что текущий путь содержит в начале предыдущий путь полностью или частично, чтобы не проходить повторно
            else {
                requestPaths = XmlUtils.getChildNodesByPathsDiff(lastSavedPath, entry.getKey(),
                        clientAttributes.get(lastSavedPath));
            }
            if (!requestPaths.isEmpty()) {
                // запоминаем последний найденный путь для последующей обработки следующего пути только по разнице двух путей
                lastSavedPath = requestPaths.entrySet().stream().findFirst().get().getKey();
            }
            if (hashPersData && XmlUtils.nodeHasPersonalDataMark(entry.getValue())) {
                requestPaths.forEach((key, value) -> XmlUtils.hashPersonalDataAttribute(value));
            }
            clientAttributes.putAll(requestPaths);
        }

        return clientAttributes;
    }

    private void hashPersonalData(XmlRequestPaths templatePaths, Document requestDocument) {
        Map<XmlPath, Node> clientAttributes = new HashMap<>();
        XmlPath lastSavedPath = null;
        // проходим в дереве запроса по сохраненным путям шаблона, по которым находятся ноды с клиентскими атрибутамм
        for (Map.Entry<XmlPath, Node> entry : templatePaths.getPaths().entrySet()) {
            // получаем пути к тегам с клиентским атрибутом
            Map<XmlPath, Node> requestPaths;

            if (lastSavedPath == null || XmlUtils.isDynamicPath(lastSavedPath) || XmlUtils.isDynamicPath(entry.getKey())) {
                // ищем с начала дерева, если пути еще не проверялись или в случае динамического пути
                requestPaths = XmlUtils.findNodesMatchingPathFromParent(requestDocument, entry.getKey());
            }
            // если ранее пути проверялись
            // проверяем, что текущий путь содержит в начале предыдущий путь полностью или частично, чтобы не проходить повторно
            else {
                requestPaths = XmlUtils.getChildNodesByPathsDiff(lastSavedPath, entry.getKey(),
                        clientAttributes.get(lastSavedPath));
            }
            if (!requestPaths.isEmpty()) {
                // запоминаем последний найденный путь для последующей обработки следующего пути только по разнице двух путей
                lastSavedPath = requestPaths.entrySet().stream().findFirst().get().getKey();
            }

            if (XmlUtils.nodeHasPersonalDataMark(entry.getValue())) {
                requestPaths.entrySet().forEach(
                        p -> XmlUtils.hashPersonalDataAttribute(p.getValue())
                );
            }
            clientAttributes.putAll(requestPaths);
        }
    }

    public String getSavedRequest(String flow, String filial, String messageId, Connection conn)
            throws DatabaseException {
        String sql = "SELECT request FROM client_attrs_requests WHERE flow=? AND filial=? AND message_id=?";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, flow);
            preparedStatement.setString(2, filial);
            preparedStatement.setString(3, messageId);

            ResultSet rs = preparedStatement.executeQuery();
            String savedRequest = null;
            if (rs.next()) {
                savedRequest = rs.getString(1);
            }

            return savedRequest;
        } catch(SQLException ex) {
            ex.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось получить данные из базы данных по сохраненному запросу:%n%s",
                    ex.getMessage() == null ? "" : ex.getMessage()));
        }
    }

    public LocalDateTime getSavedRequestLastUpdate(String flow, String filial, String messageId, Connection conn)
            throws DatabaseException {
        String sql = "SELECT updated_at FROM client_attrs_requests WHERE flow=? AND filial=? AND message_id=?";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, flow);
            preparedStatement.setString(2, filial);
            preparedStatement.setString(3, messageId);

            ResultSet rs = preparedStatement.executeQuery();
            LocalDateTime lastUpdate = null;
            if (rs.next()) {
                lastUpdate = rs.getObject(1, LocalDateTime.class);
            }

            return lastUpdate;
        } catch(Exception ex) {
            ex.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось получить данные из базы данных по дате обновления сохраненного запроса:%n%s",
                    ex.getMessage() == null ? "" : ex.getMessage()));
        }
    }

    public void saveRequest(ControlTypeRequest controlTypeRequest, Connection conn)
            throws RequestControlServiceException {

        LocalDateTime timestamp = ZonedDateTime.now()
                .withZoneSameInstant(ZoneId.systemDefault())
                .toLocalDateTime();
        String flow = controlTypeRequest.getFlowType().name();

        Document preparedDoc;
        // парсим xml-строку в дерево
        try {
            preparedDoc = XmlUtils.getDocumentFromXmlString(controlTypeRequest.getRequest());
        } catch (ParserConfigurationException | IOException | SAXException e) {
            throw new RequestControlServiceException("Не удалось обработать xml структуру запроса на сохранение.");
        }

        // хешируем перс данные
        try {
            hashPersonalData(getCurrentTemplate(flow, timestamp, conn), preparedDoc);
        } catch (RequestControlServiceException e) {
            throw new RequestControlServiceException(String.format(
                    "Не удалось выполнить хеширование персональных данных. %n%s",
                    e.getMessage() == null ? "" : e.getMessage()));
        }

        saveOrUpdateRequest(controlTypeRequest, preparedDoc, timestamp, conn);
    }

    @Transactional
    public void saveOrUpdateRequest(ControlTypeRequest controlTypeRequest,
                                     Document preparedDoc,
                                     LocalDateTime timestamp,
                                     Connection conn) {
        String flow = controlTypeRequest.getFlowType().name();
        String filial = controlTypeRequest.getSegment();
        String masterId = controlTypeRequest.getRequestIdentifiers().get(ID_MASTER_SYSTEM).getIdValue();

        String request;
        // парсим дерево в строку для сохранения в бд
        try {
            request = XmlUtils.getXmlStringFromDocument(preparedDoc);
        } catch (TransformerException e) {
            e.printStackTrace();
            throw new IllegalArgumentException(String.format(
                    "Не удалось обработать xml структуру запроса на сохранение:%n%s",
                    e.getMessage() == null ? "" : e.getMessage()));
        }

        try {
            LocalDateTime lastUpdatedAt = getSavedRequestLastUpdate(flow, filial, masterId, conn);
            String sql;
            if (lastUpdatedAt == null) {
                sql = "INSERT INTO client_attrs_requests (flow, filial, message_id, updated_at, request) VALUES (?,?,?,?,?)";
                try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                    preparedStatement.setString(1, flow);
                    preparedStatement.setString(2, filial);
                    preparedStatement.setString(3, masterId);
                    preparedStatement.setObject(4, timestamp, Types.TIMESTAMP);
                    preparedStatement.setString(5, request);

                    preparedStatement.executeUpdate();
                }
            } else {
                sql = "UPDATE client_attrs_requests SET updated_at=?, request=? WHERE flow=? AND filial=? AND message_id=?";
                try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                    preparedStatement.setObject(1, timestamp, Types.TIMESTAMP);
                    preparedStatement.setString(2, request);
                    preparedStatement.setString(3, flow);
                    preparedStatement.setString(4, filial);
                    preparedStatement.setString(5, masterId);

                    preparedStatement.executeUpdate();
                }
            }
        } catch(SQLException e) {
            e.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось сохранить запрос в базе данных:%n%s",
                    e.getMessage() == null ? "" : e.getMessage()));
        }
    }

    public String getEffectiveTemplateName(String flow, LocalDateTime timestamp, Connection conn)
            throws DatabaseException {
            String sql = "SELECT name FROM client_attrs_templates where flow=? and created_at < ? ORDER BY created_at DESC LIMIT 1";
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                preparedStatement.setString(1, flow);
                preparedStatement.setObject(2, timestamp, Types.TIMESTAMP);

                ResultSet rs = preparedStatement.executeQuery();
                String templateName = null;
                if (rs.next()) {
                    templateName = rs.getString(1);
                }

                return templateName;
        } catch(SQLException ex) {
            ex.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось получить данные из базы данных о наименовании актуального шаблона:%n%s",
                    ex.getMessage() == null ? "" : ex.getMessage()));
        }
    }

    public List<String> getAllEffectiveTemplateNames(Connection conn) throws DatabaseException {
        String sql = "SELECT DISTINCT flow, MAX(created_at) FROM client_attrs_templates GROUP BY flow";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            ResultSet rs = preparedStatement.executeQuery();
            List<String> templateNames = new ArrayList<>();
            while (rs.next()) {
                templateNames.add(rs.getString(1));
            }

            return templateNames;
        } catch(SQLException ex) {
            ex.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось получить данные из базы данных о всех актуальных шаблонах:%n%s",
                    ex.getMessage() == null ? "" : ex.getMessage()));
        }
    }

    public XmlRequestTemplate getEffectiveTemplate(String flow, LocalDateTime date, Connection conn)
            throws DatabaseException, ClientAttributesDataException {
        String sql = "SELECT name, template FROM client_attrs_templates where flow=? and created_at < ? ORDER BY created_at DESC LIMIT 1";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, flow);
            preparedStatement.setObject(2, date, Types.TIMESTAMP);

            ResultSet rs = preparedStatement.executeQuery();
            XmlRequestTemplate template = null;
            Document templateDocument = null;
            if (rs.next()) {
                String templateName = rs.getString(1);
                String templateStr = rs.getString(2);

                templateDocument = XmlUtils.getDocumentFromXmlString(templateStr);
                Map<XmlPath, Node> templatePaths = XmlUtils.getTemplatePathsFromNode(
                        new XmlPath(), templateDocument.getFirstChild());
                template = new XmlRequestTemplate(templateName, templatePaths);
            }

            return template;
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось получить данные из базы данных об актуальном шаблоне:%n%s",
                    ex.getMessage() == null ? "" : ex.getMessage()));
        } catch (ParserConfigurationException | IOException | SAXException e) {
            e.printStackTrace();
            throw new ClientAttributesDataException(String.format(
                    "Ошибка обработки шаблона клиентских атрибутов.%n%s",
                    e.getMessage() == null ? "" : e.getMessage()));
        }
    }

    public static String getRequestIdentifier(String flow, String filial, String messageId) {
        return flow +
                "." +
                filial +
                "." +
                messageId;
    }

    @Transactional
    public void saveTemplate(String flow, LocalDateTime timestamp, String template, Connection conn)
            throws DatabaseException {
        String sql = "INSERT INTO client_attrs_templates (flow, created_at, name, template) VALUES(?,?,?,?)";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {

            preparedStatement.setString(1, flow);
            preparedStatement.setObject(2, timestamp, Types.TIMESTAMP);
            preparedStatement.setString(3, generateTemplateName(flow, timestamp));
            preparedStatement.setString(4, template);

            preparedStatement.executeUpdate();
        } catch(SQLException ex) {
            ex.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось сохранить данные о шаблоне в базе данных:%n%s",
                    ex.getMessage() == null ? "" : ex.getMessage()));
        }
    }

    private static String generateTemplateName(String flow, LocalDateTime timestamp) {
        return flow + "-" + timestamp;
    }

    @Scheduled(fixedRateString = "${service.client-attrs-cache.time}")
    public void cleanSavedRequestsCache() {
        log.info("Start clean expired saved requests cache");
        savedRequestsCache.cleanExpiredCache();
    }
}
