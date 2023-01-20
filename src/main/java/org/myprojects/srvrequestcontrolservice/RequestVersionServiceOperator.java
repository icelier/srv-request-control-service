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
import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
@Component
public class RequestVersionServiceOperator extends AbstractServiceOperator {

    @Autowired
    private DataSource dataSource;

    @Override
    public void updateServiceData(ControlTypeRequest controlTypeRequest, Connection conn)
            throws RequestControlServiceException {
        updateRequestVersionInDB(controlTypeRequest, conn);
    }

    @Override
    public ControlTypeResult doServiceCheck(ControlTypeRequest controlTypeRequest, Connection conn) {
        return checkRequestVersion(controlTypeRequest, conn);
    }

    @Override
    public boolean validateRequest(ServiceRequest serviceRequest) {
        return serviceRequest.getRequestVersion() != null
                && serviceRequest.getIdList() != null
                && serviceRequest.getIdList().getRequestIds() != null
                && !serviceRequest.getIdList().getRequestIds().isEmpty()
                && !StringUtils.isBlank(serviceRequest.getIdList().get(RequestIdentifier.Id.ID_INTEGRATION).getIdValue())
                && (
                        (serviceRequest.getIdList().contains(RequestIdentifier.Id.ID_MASTER_SYSTEM)
                                && !StringUtils.isBlank(serviceRequest.getIdList().get(RequestIdentifier.Id.ID_MASTER_SYSTEM).getIdValue()))
                                || (serviceRequest.getIdList().contains(RequestIdentifier.Id.ID_FILIAL)
                                && !StringUtils.isBlank(serviceRequest.getIdList().get(RequestIdentifier.Id.ID_FILIAL).getIdValue()))
                );
    }

    public ControlTypeResult checkRequestVersion(ControlTypeRequest controlTypeRequest, Connection conn) {
        Operators.ControlType controlType = controlTypeRequest.getControlType();

        Integer savedRequestVersion;
        try {
            savedRequestVersion = getRequestVersionFromDB(controlTypeRequest, conn);

            if (savedRequestVersion == null) {
                if (controlTypeRequest.getRequestVersion() == 1
                        || (controlTypeRequest.getRequestIdentifiers().get(RequestIdentifier.Id.ID_INTEGRATION) != null
                        && controlTypeRequest.getRequestIdentifiers().getValue(RequestIdentifier.Id.ID_INTEGRATION) != null
                        && !controlTypeRequest.getRequestIdentifiers().getValue(RequestIdentifier.Id.ID_INTEGRATION).isBlank())) {
                    return new ControlTypeResult(controlType);
                } else {
                    // если в БД ничего нет, возвращаем ошибку
                    return new ControlTypeResult(controlType, ServiceResponse.Status.ERROR,
                            "Номер версии создаваемого обращения должен быть равен 1");
                }
            } else if (savedRequestVersion < controlTypeRequest.getRequestVersion()) {
            // если нет ошибок, возвращаем статус ОК
                return new ControlTypeResult(controlType);
            } else {
                // если пришла старая версия заявки, возвращаем ошибку
                return new ControlTypeResult(controlType, ServiceResponse.Status.ERROR, String.format(
                        "Версия передаваемого обращения устарела. Пожалуйста, получите актуальную версию '%d' перед обновлением",
                                savedRequestVersion));
            }
        } catch (RequestControlServiceException e) {
            return new ControlTypeResult(controlType, ServiceResponse.Status.FAILED,
                    e.getMessage() == null ? "" : e.getMessage());
        }
    }

    public Integer getRequestVersionFromDB(ControlTypeRequest controlTypeRequest, Connection conn)
            throws DatabaseException {
        Integer requestVersion = null;
        // сначала пытаемся запросить по id Мастер-системы
        if (controlTypeRequest.getRequestIdentifiers().
                get(RequestIdentifier.Id.ID_MASTER_SYSTEM) != null
                && !StringUtils.isBlank(controlTypeRequest.getRequestIdentifiers().
                get(RequestIdentifier.Id.ID_MASTER_SYSTEM).getIdValue())) {
            requestVersion = getRequestVersionForMasterSystemRequest(controlTypeRequest, conn);
        }
        // если id Мастер-системы не было в запросе (например, для заявок Филиала) или по нему ничего не нашлось, ищем по id Филиала
        if (requestVersion == null && controlTypeRequest.getRequestIdentifiers().
                get(RequestIdentifier.Id.ID_FILIAL) != null
                && !StringUtils.isBlank(controlTypeRequest.getRequestIdentifiers().
                get(RequestIdentifier.Id.ID_FILIAL).getIdValue())) {
            requestVersion = getRequestVersionForFilialRequest(controlTypeRequest, conn);
        }

        return requestVersion;
    }

    private Integer getRequestVersionForMasterSystemRequest(ControlTypeRequest controlTypeRequest,
                                                      Connection conn) throws DatabaseException {
        String sql = "SELECT request_version FROM request_identifiers WHERE flow=? AND filial=? AND id_master_system=? AND id_integration=?";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            RequestControlService.setPreparedStatementStringParams(preparedStatement,
                    controlTypeRequest.getFlowType().name(),
                    controlTypeRequest.getSegment(),
                    controlTypeRequest.getRequestIdentifiers().get(
                            RequestIdentifier.Id.ID_MASTER_SYSTEM).getIdValue(),
                    controlTypeRequest.getRequestIdentifiers().get(
                            RequestIdentifier.Id.ID_INTEGRATION).getIdValue());

            ResultSet rs = preparedStatement.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось получить данные по версии обращения из базы данных.%n%s",
                    e.getMessage() == null ? "" : e.getMessage()));
        }

        return null;
    }

    private Integer getRequestVersionForFilialRequest(ControlTypeRequest controlTypeRequest,
                                                      Connection conn) throws DatabaseException {
        String sql = "SELECT request_version FROM request_identifiers WHERE flow=? AND filial=? AND id_filial=? AND id_integration=?";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            RequestControlService.setPreparedStatementStringParams(preparedStatement,
                    controlTypeRequest.getFlowType().name(),
                    controlTypeRequest.getSegment(),
                    controlTypeRequest.getRequestIdentifiers().get(
                            RequestIdentifier.Id.ID_FILIAL).getIdValue(),
                    controlTypeRequest.getRequestIdentifiers().get(
                            RequestIdentifier.Id.ID_INTEGRATION).getIdValue());

            ResultSet rs = preparedStatement.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось получить данные по версии обращения из базы данных.%n%s",
                    e.getMessage() == null ? "" : e.getMessage()));
        }

        return null;
    }

    public void updateRequestVersionInDB(ControlTypeRequest controlTypeRequest, Connection conn)
            throws RequestControlServiceException {
        IdList identifiersFromDb = RequestControlService.getIdentifiersFromDB(controlTypeRequest, conn);

        if (identifiersFromDb.getRequestIds().isEmpty()) {
            insertVersionToDb(controlTypeRequest, conn);
        } else {
            updateVersionInDb(controlTypeRequest, conn);
        }
    }

    @Transactional
    public void insertVersionToDb(ControlTypeRequest controlTypeRequest, Connection conn)
            throws DatabaseException {
        String sql = "INSERT INTO request_identifiers (flow, filial, request_version, id_integration, id_master_system, id_filial, filial_id, id_main_check_system) VALUES(?,?,?,?,?,?,?,?)";
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

            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new DatabaseException(String.format(
                    "Не удалось сохранить данные в базу данных:%n%s",
                    ex.getMessage()));
        }
    }

    @Transactional
    public void updateVersionInDb(ControlTypeRequest controlTypeRequest, Connection conn)
            throws RequestControlServiceException {
        PreparedStatement preparedStatement = null;
        int updateCount = 0;
        try {
            String sql;
            // сначала пытаемся запросить по id Мастер-системы
            if (controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_MASTER_SYSTEM) != null
                    && !StringUtils.isBlank(controlTypeRequest.getRequestIdentifiers().
                    get(RequestIdentifier.Id.ID_MASTER_SYSTEM).getIdValue())) {
                sql = "UPDATE request_identifiers SET request_version=? WHERE flow=? AND filial=? AND id_master_system=? AND filial_id=?";
                preparedStatement = conn.prepareStatement(sql);
                preparedStatement.setInt(1, controlTypeRequest.getRequestVersion());
                RequestControlService.setPreparedStatementStringParams(preparedStatement, 1,
                        controlTypeRequest.getFlowType().name(),
                        controlTypeRequest.getSegment(),
                        controlTypeRequest.getRequestIdentifiers().get(
                                RequestIdentifier.Id.ID_MASTER_SYSTEM).getIdValue(),
                        controlTypeRequest.getRequestIdentifiers().get(
                                RequestIdentifier.Id.FILIAL_ID).getIdValue());

                updateCount = preparedStatement.executeUpdate();
            }
            // если id Мастер-системы не было в запросе (например, для заявок Филиала)
            // или по нему ничего не нашлось, ищем по id Филиала
            if (updateCount == 0 && controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_FILIAL) != null
                    && !StringUtils.isBlank(controlTypeRequest.getRequestIdentifiers().get(
                    RequestIdentifier.Id.ID_FILIAL).getIdValue())) {
                sql = "UPDATE request_identifiers SET request_version=? WHERE flow=? AND filial=? AND id_filial=? AND filial_id=?";
                preparedStatement = conn.prepareStatement(sql);
                preparedStatement.setInt(1, controlTypeRequest.getRequestVersion());
                RequestControlService.setPreparedStatementStringParams(preparedStatement, 1,
                        controlTypeRequest.getFlowType().name(),
                        controlTypeRequest.getSegment(),
                        controlTypeRequest.getRequestIdentifiers().get(
                                RequestIdentifier.Id.ID_FILIAL).getIdValue(),
                        controlTypeRequest.getRequestIdentifiers().get(
                                RequestIdentifier.Id.FILIAL_ID).getIdValue());

                updateCount = preparedStatement.executeUpdate();
            }

            if (updateCount == 0) {
                throw new RequestControlServiceException("Обновление версии обращения не выполнено.");
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RequestControlServiceException(String.format(
                    "Не удалось сохранить версию обращения в базу данных:%n%s",
                    ex.getMessage() == null ? "" : ex.getMessage()));
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    log.warn("PreparedStatement не был закрыт должным образом.");
                }
            }
        }
    }
}
