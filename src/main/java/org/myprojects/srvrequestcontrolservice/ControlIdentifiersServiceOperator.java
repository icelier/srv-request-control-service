package org.myprojects.srvrequestcontrolservice;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.myprojects.srvrequestcontrolservice.data.*;
import org.myprojects.srvrequestcontrolservice.exceptions.DatabaseException;
import org.myprojects.srvrequestcontrolservice.exceptions.RequestControlServiceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class ControlIdentifiersServiceOperator extends AbstractServiceOperator {

    @Autowired
    private DataSource dataSource;

    @Override
    public void updateServiceData(ControlTypeRequest controlTypeRequest, Connection conn) {
        saveIdentifiersToDB(controlTypeRequest, conn);
    }

    @Override
    public ControlTypeResult doServiceCheck(ControlTypeRequest controlTypeRequest, Connection conn) {
        return checkControlIdentifiers(controlTypeRequest, conn);
    }

    @Override
    public boolean validateRequest(ServiceRequest serviceRequest) {
        return serviceRequest.getRequestVersion() != null
                && serviceRequest.getIdList() != null
                && serviceRequest.getIdList().getRequestIds() != null
                && !serviceRequest.getIdList().getRequestIds().isEmpty()
                && serviceRequest.getIdList().contains(RequestIdentifier.Id.ID_INTEGRATION)
                && !StringUtils.isBlank(serviceRequest.getIdList().get(RequestIdentifier.Id.ID_INTEGRATION).getIdValue())
                && (
                        (serviceRequest.getIdList().contains(RequestIdentifier.Id.ID_MASTER_SYSTEM)
                                && !StringUtils.isBlank(serviceRequest.getIdList().get(RequestIdentifier.Id.ID_MASTER_SYSTEM).getIdValue()))
                                || (serviceRequest.getIdList().contains(RequestIdentifier.Id.ID_FILIAL)
                                && !StringUtils.isBlank(serviceRequest.getIdList().get(RequestIdentifier.Id.ID_FILIAL).getIdValue()))
        );
    }

//    @Transactional
    public ControlTypeResult checkControlIdentifiers(ControlTypeRequest controlTypeRequest,
                                                     Connection conn) {
        IdList identifiersFromDB;
        List<String> errors;
        try {
            identifiersFromDB = RequestControlService.getIdentifiersFromDB(controlTypeRequest, conn);
            if (identifiersFromDB.getRequestIds().isEmpty()) {
                // идентификаторы могут быть пустые для версии 1 обращения или
                // в случае первого заупска сервиса, когда в бд еще нет сохраненных данных
                if (controlTypeRequest.getRequestVersion() == 1
                || (controlTypeRequest.getRequestIdentifiers().get(RequestIdentifier.Id.ID_INTEGRATION) != null
                && !StringUtils.isBlank(controlTypeRequest.getRequestIdentifiers().getValue(RequestIdentifier.Id.ID_INTEGRATION)))) {
                    return new ControlTypeResult(controlTypeRequest.getControlType());
                } else {
                    return new ControlTypeResult(controlTypeRequest.getControlType(), ServiceResponse.Status.ERROR,
                            "Обращение с указанными идентификаторами не найдено");
                }
            }

            errors = checkIdentifiers(identifiersFromDB, controlTypeRequest, conn);

            return errors.isEmpty() ? new ControlTypeResult(controlTypeRequest.getControlType())
                    : new ControlTypeResult(controlTypeRequest.getControlType(), ServiceResponse.Status.ERROR,
                    String.join(";" + System.lineSeparator(), errors));
        } catch (RequestControlServiceException e) {
            return new ControlTypeResult(controlTypeRequest.getControlType(), ServiceResponse.Status.FAILED,
                    e.getMessage() == null ? "" : e.getMessage());
        }
    }

//    @Transactional
    public List<String> checkIdentifiers(IdList identifiersFromDB,
                                         ControlTypeRequest controlTypeRequest,
                                         Connection conn) {
        IdList requestIdentifiers = controlTypeRequest.getRequestIdentifiers();

        // обновляем в БД те идентификаторы, которые ранее не приходили
        updateMissingIdentifiers(controlTypeRequest, identifiersFromDB, conn);

        List<String> errors = new ArrayList<>();
        // проверяем идентификаторы, которые были в БД

        for (RequestIdentifier id : identifiersFromDB.getRequestIds()) {
            if (requestIdentifiers.get(id.getIdName()) != null
                    && !StringUtils.isBlank(requestIdentifiers.get(id.getIdName()).getIdValue())) {
                if (!requestIdentifiers.get(id.getIdName()).getIdValue()
                        .equals(id.getIdValue())) {
                    switch (id.getIdName()) {
                        case REQUEST_TYPE_ID:
                            errors.add("Тип обращения не может быть изменен");
                            break;
                        case ID_INTEGRATION:
                            errors.add("Некорректное значение идентификатора Id-Integration");
                            break;
                        case ID_MAIN_CHECK_SYSTEM:
                            errors.add("Некорректное значение идентификатора Id-MainCheckSystem");
                            break;
                        case ID_MASTER_SYSTEM:
                            errors.add("Некорректное значение идентификатора Id-Мастер-системы");
                            break;
                        case ID_FILIAL:
                            errors.add("Некорректное значение идентификатора Id-Филиала");
                            break;
                        case FILIAL_ID:
                            errors.add("Некорректное значение кода орг. структуры");
                            break;
                        default:
                                throw new IllegalArgumentException(
                                        String.format("Идентификатор %s не входит в список проверки.", id.getIdName()));
                    }
                }
            } else {
                switch (id.getIdName()) {
                    case REQUEST_TYPE_ID:
                        errors.add("Не указан тип обращения");
                        break;
                    case ID_INTEGRATION:
                        errors.add("Отсутствует идентификатор Id-Integration");
                        break;
                    case ID_MAIN_CHECK_SYSTEM:
                        errors.add("Отсутствует идентификатор Id-MainCheckSystem");
                        break;
                    case ID_MASTER_SYSTEM:
                        errors.add("Отсутствует идентификатор Id-Мастер-системы");
                        break;
                    case ID_FILIAL:
                        errors.add("Отсутствует идентификатор Id-Филиала");
                        break;
                    case FILIAL_ID:
                        errors.add("Отсутствует код орг. структуры");
                        break;
                    default:
                        throw new IllegalArgumentException(
                                String.format("Идентификатор %s не входит в список проверки.", id.getIdName()));
                }
            }
        }

        return errors;
    }

    public void saveIdentifiersToDB(ControlTypeRequest controlTypeRequest, Connection conn)
            throws RequestControlServiceException {
        IdList identifiersFromDb = RequestControlService.getIdentifiersFromDB(controlTypeRequest, conn);

        if (identifiersFromDb.getRequestIds().isEmpty()) {
            insertIdentifiersToDb(controlTypeRequest, conn);
        } else {
            updateMissingIdentifiers(controlTypeRequest, identifiersFromDb, conn);
        }
    }

    @Transactional
    public void insertIdentifiersToDb(ControlTypeRequest controlTypeRequest, Connection conn)
            throws DatabaseException {
        String sql = "INSERT INTO request_identifiers (flow, filial, id_integration, id_master_system, id_filial, filial_id, id_main_check_system, request_type_id) VALUES(?,?,?,?,?,?,?,?)";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, controlTypeRequest.getFlowType().name());
            preparedStatement.setString(2, controlTypeRequest.getSegment());
            preparedStatement.setString(3, controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_INTEGRATION) == null ? null : controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_INTEGRATION).getIdValue());
            preparedStatement.setString(4, controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_MASTER_SYSTEM) == null ? null : controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_MASTER_SYSTEM).getIdValue());
            preparedStatement.setString(5, controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_FILIAL) == null ? null : controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_FILIAL).getIdValue());
            preparedStatement.setString(6, controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.FILIAL_ID) == null ? null : controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.FILIAL_ID).getIdValue());
            preparedStatement.setString(7, controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_MAIN_CHECK_SYSTEM) == null ? null : controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_MAIN_CHECK_SYSTEM).getIdValue());
            preparedStatement.setString(8, controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.REQUEST_TYPE_ID) == null ? null : controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.REQUEST_TYPE_ID).getIdValue());

            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось сохранить данные в базу данных:%n%s",
                    ex.getMessage() == null ? "" : ex.getMessage()));
        }
    }

    @Transactional
    public void updateMissingIdentifiers(ControlTypeRequest controlTypeRequest,
                                         IdList identifiersFromDb,
                                         Connection conn)
            throws RequestControlServiceException {
        IdList requestIdentifiers = controlTypeRequest.getRequestIdentifiers();

        List<String> params = new ArrayList<>();
        // отфитровываем уже сохраненные в бд идентификаторы
        List<RequestIdentifier> notEmptyIdentifiersFromDb = identifiersFromDb.getRequestIds().stream()
                .filter(id -> id.getIdValue() != null && !id.getIdValue().isBlank())
                .collect(Collectors.toList());
        // отфитровываем те идентификаторы, которые не были сохранены в бд
        List<RequestIdentifier> emptyIdentifiersFromDb = identifiersFromDb.getRequestIds().stream()
                .filter(id -> id.getIdValue() == null || id.getIdValue().isBlank())
                .filter(id -> requestIdentifiers.contains(id.getIdName()))
                .collect(Collectors.toList());

        if (emptyIdentifiersFromDb.isEmpty()) {
            // все идентификтаоры уже сохранены
            return;
        }

        // формируем запрос
        StringBuilder sqlBuilder = new StringBuilder();
        for (RequestIdentifier id : emptyIdentifiersFromDb) {
            if (requestIdentifiers.get(id.getIdName()) != null) {
                if (!sqlBuilder.toString().isEmpty()) {
                    sqlBuilder.append(", ");
                }
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

        try (PreparedStatement ps = conn.prepareStatement("UPDATE request_identifiers SET " + sqlBuilder)) {
            for (int i = 0; i < params.size(); i++) {
                ps.setString(i + 1, params.get(i));
            }

            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось обновить данные по идентификаторам в базе данных:%n%s",
                    e.getMessage() == null ? "" : e.getMessage()));
        }
    }
}
