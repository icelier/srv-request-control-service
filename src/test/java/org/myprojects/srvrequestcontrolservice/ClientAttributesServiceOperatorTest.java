package org.myprojects.srvrequestcontrolservice;

import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.myprojects.srvrequestcontrolservice.data.*;
import org.myprojects.srvrequestcontrolservice.utils.TempCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.core.io.Resource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.FileCopyUtils;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@RunWith(SpringRunner.class)
@SpringBootTest
@SpringJUnitConfig(BeansConfig.class)
public class ClientAttributesServiceOperatorTest {

    @Autowired
    private ClientAttributesServiceOperator clientAttributesServiceOperator;

    @Value("classpath:request.xml")
    Resource requestFile;
    @Value("classpath:valid_request.xml")
    Resource validRequestFile;
    @Value("classpath:invalid_request.xml")
    Resource invalidRequestFile;
    @Value("classpath:template.xml")
    Resource templateFile;
    @Value("classpath:create.sql")
    Resource sqlFile;
    @Value("${service.client-attrs-cache.time}")
    long cacheTimePeriod;

    @Autowired
    DataSource dataSource;

    @SpyBean
    TempCache<TempCache.Unit<ParsedXmlRequest>> savedRequestCache;

    String templateStr;
    String reqStr;
    String validReqStr;
    String invalidReqStr;
    LocalDateTime saveTimestamp = LocalDateTime.now();
    String sql;

    String testFlow = ServiceRequest.DataFlowType.DATA_FLOW_TYPE_1.name();
    String testSegment = "TEST_SEGMENT";
    String testMasterId = "TEST_MASTER_ID";

    @Before
    public void init() throws Exception {
        InputStream is = templateFile.getInputStream();
        Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
        templateStr = FileCopyUtils.copyToString(reader);

        is.close();
        reader.close();
        is = requestFile.getInputStream();
        reader = new InputStreamReader(is, StandardCharsets.UTF_8);
        reqStr = FileCopyUtils.copyToString(reader);

        is.close();
        reader.close();
        is = validRequestFile.getInputStream();
        reader = new InputStreamReader(is, StandardCharsets.UTF_8);
        validReqStr = FileCopyUtils.copyToString(reader);

        is.close();
        reader.close();
        is = invalidRequestFile.getInputStream();
        reader = new InputStreamReader(is, StandardCharsets.UTF_8);
        invalidReqStr = FileCopyUtils.copyToString(reader);

        is.close();
        reader.close();
        is = sqlFile.getInputStream();
        reader = new InputStreamReader(is, StandardCharsets.UTF_8);
        sql = FileCopyUtils.copyToString(reader);

        try (Connection conn = dataSource.getConnection()) {
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        preparedStatement.executeUpdate();

            clientAttributesServiceOperator.saveTemplate(testFlow, saveTimestamp, templateStr, conn);
        }
        ControlTypeRequest controlTypeRequest = new ControlTypeRequest.Builder(Operators.ControlType.CLIENT_ATTRIBUTES,
                ServiceRequest.DataFlowType.DATA_FLOW_TYPE_1, testSegment)
                .setRequestIdentifiers(new IdList(List.of(new RequestIdentifier(RequestIdentifier.Id.ID_MASTER_SYSTEM, testMasterId))))
                .setRequest(reqStr).build();
        clientAttributesServiceOperator.saveRequest(controlTypeRequest, dataSource.getConnection());
    }

    @Test
    public void givenCheckAndSavedRequestWithMatchingClientAttributes_resultIsOK() throws SQLException {
        ControlTypeRequest checkRequest = new ControlTypeRequest.Builder(Operators.ControlType.CLIENT_ATTRIBUTES,
                ServiceRequest.DataFlowType.DATA_FLOW_TYPE_1, testSegment)
                .setRequestIdentifiers(new IdList(List.of(new RequestIdentifier(RequestIdentifier.Id.ID_MASTER_SYSTEM, testMasterId))))
                .setRequest(validReqStr).build();

        ControlTypeResult checkResponse;
        try (Connection conn = dataSource.getConnection()) {
            checkResponse = clientAttributesServiceOperator.checkClientAttributes(checkRequest, conn);
        }

        Assertions.assertSame(ServiceResponse.Status.OK, checkResponse.getStatus());
    }

    @Test
    public void givenCheckAndSavedRequestWithUnmatchingClientAttributes_resultIsError() throws SQLException {
        ControlTypeRequest checkRequest = new ControlTypeRequest.Builder(Operators.ControlType.CLIENT_ATTRIBUTES,
                ServiceRequest.DataFlowType.DATA_FLOW_TYPE_1, testSegment)
                .setRequestIdentifiers(new IdList(List.of(new RequestIdentifier(RequestIdentifier.Id.ID_MASTER_SYSTEM, testMasterId))))
                .setRequest(invalidReqStr).build();

        ControlTypeResult checkResponse;
        try (Connection conn = dataSource.getConnection()) {
            checkResponse = clientAttributesServiceOperator.checkClientAttributes(checkRequest, conn);
        }

        Assertions.assertSame(ServiceResponse.Status.ERROR, checkResponse.getStatus());
        Assertions.assertTrue(checkResponse.getErrorDescription().contains("ClientEmail description"));
        Assertions.assertTrue(checkResponse.getErrorDescription().contains("operationMonth description"));
    }

    @Test
    public void givenCheckRequestHasClientAttributeThatIsAbsentInSavedRequest_resultIsError() throws SQLException {
        ControlTypeRequest checkRequest = new ControlTypeRequest.Builder(Operators.ControlType.CLIENT_ATTRIBUTES,
                ServiceRequest.DataFlowType.DATA_FLOW_TYPE_1, testSegment)
                .setRequestIdentifiers(new IdList(List.of(new RequestIdentifier(RequestIdentifier.Id.ID_MASTER_SYSTEM, testMasterId))))
                .setRequest(invalidReqStr).build();

        ControlTypeResult checkResponse;
        try (Connection conn = dataSource.getConnection()) {
            checkResponse = clientAttributesServiceOperator.checkClientAttributes(checkRequest, conn);
        }

        Assertions.assertSame(ServiceResponse.Status.ERROR, checkResponse.getStatus());
        Assertions.assertTrue(checkResponse.getErrorDescription().contains("FeedMethod description"));
    }

    @Test
    public void givenCheckRequestChangedNonClientAttribute_resultHasNoError() throws SQLException {
        ControlTypeRequest checkRequest = new ControlTypeRequest.Builder(Operators.ControlType.CLIENT_ATTRIBUTES,
                ServiceRequest.DataFlowType.DATA_FLOW_TYPE_1, testSegment)
                .setRequestIdentifiers(new IdList(List.of(new RequestIdentifier(RequestIdentifier.Id.ID_MASTER_SYSTEM, testMasterId))))
                .setRequest(invalidReqStr).build();

        ControlTypeResult checkResponse;
        try (Connection conn = dataSource.getConnection()) {
            checkResponse = clientAttributesServiceOperator.checkClientAttributes(checkRequest, conn);
        }

        Assertions.assertFalse(checkResponse.getErrorDescription().contains("RequestId_MasterSystem description"));
    }

    @Test
    public void givenCheckRequestNotContainsClientAttributes_resultIsOK() throws SQLException {
        ControlTypeRequest checkRequest = new ControlTypeRequest.Builder(Operators.ControlType.CLIENT_ATTRIBUTES,
                ServiceRequest.DataFlowType.DATA_FLOW_TYPE_1, testSegment)
                .setRequestIdentifiers(new IdList(List.of(new RequestIdentifier(RequestIdentifier.Id.ID_MASTER_SYSTEM, testMasterId))))
                .setRequest(invalidReqStr).build();

        ControlTypeResult checkResponse;
        try (Connection conn = dataSource.getConnection()) {
            checkResponse = clientAttributesServiceOperator.checkClientAttributes(checkRequest, conn);
        }

        Assertions.assertFalse(checkResponse.getErrorDescription().contains("attr1 description"));
        Assertions.assertFalse(checkResponse.getErrorDescription().contains("attr2 description"));
    }

    @Test
    public void givenCheckRequestHasMultipleSectionWithChangedClientAttributes_resultHasErrors() throws SQLException {
        ControlTypeRequest checkRequest = new ControlTypeRequest.Builder(Operators.ControlType.CLIENT_ATTRIBUTES,
                ServiceRequest.DataFlowType.DATA_FLOW_TYPE_1, testSegment)
                .setRequestIdentifiers(new IdList(List.of(new RequestIdentifier(RequestIdentifier.Id.ID_MASTER_SYSTEM, testMasterId))))
                .setRequest(invalidReqStr).build();

        ControlTypeResult checkResponse;
        try (Connection conn = dataSource.getConnection()) {
            checkResponse = clientAttributesServiceOperator.checkClientAttributes(checkRequest, conn);
        }

        Assertions.assertTrue(checkResponse.getErrorDescription().contains("operationMonth description"));
        Assertions.assertTrue(checkResponse.getErrorDescription().contains("designMonth description"));
    }

    @Test
    public void givenSleepByCacheCleanPeriod_savedRequestCacheIsEmpty() throws SQLException {
        ControlTypeRequest checkRequest = new ControlTypeRequest.Builder(Operators.ControlType.CLIENT_ATTRIBUTES,
                ServiceRequest.DataFlowType.DATA_FLOW_TYPE_1, testSegment)
                .setRequestIdentifiers(new IdList(List.of(new RequestIdentifier(RequestIdentifier.Id.ID_MASTER_SYSTEM, testMasterId))))
                .setRequest(validReqStr).build();

        ControlTypeResult checkResponse;
        try (Connection conn = dataSource.getConnection()) {
            checkResponse = clientAttributesServiceOperator.checkClientAttributes(checkRequest, conn);
        }

        Assertions.assertNotNull(savedRequestCache.getCachedUnit(ClientAttributesServiceOperator.getRequestIdentifier(
                testFlow,
                testSegment, testMasterId)));

        await()
                .atMost(cacheTimePeriod*3, TimeUnit.MILLISECONDS)
                .until(() -> savedRequestCache.getSize() == 0);
    }
}
