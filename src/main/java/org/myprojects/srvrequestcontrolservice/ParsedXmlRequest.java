package org.myprojects.srvrequestcontrolservice;

import org.w3c.dom.Node;

import java.time.LocalDateTime;
import java.util.Map;

public class ParsedXmlRequest extends XmlRequestPaths {

    private String flow;
    private String filial;
    private String messageId;
    private LocalDateTime lastUpdated;

    public ParsedXmlRequest(String flow, String filial, String messageId, LocalDateTime lastUpdated, Map<XmlPath, Node> clientAttrPaths) {
        super(clientAttrPaths);
        this.flow = flow;
        this.filial = filial;
        this.messageId = messageId;
        this.lastUpdated = lastUpdated;
    }

    public ParsedXmlRequest(String flow, String filial, String messageId, Map<XmlPath, Node> clientAttrPaths) {
        this(flow, filial, messageId, null, clientAttrPaths);
        this.flow = flow;
        this.filial = filial;
        this.messageId = messageId;
    }

    public String getFlow() {
        return flow;
    }

    public String getSegment() {
        return filial;
    }

    public String getMessageId() {
        return messageId;
    }

    public LocalDateTime getLastUpdated() {
        return lastUpdated;
    }
}
