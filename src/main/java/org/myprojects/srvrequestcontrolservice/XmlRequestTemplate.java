package org.myprojects.srvrequestcontrolservice;

import org.w3c.dom.Node;

import java.util.Map;

public class XmlRequestTemplate extends XmlRequestPaths {

    private String name;

    public XmlRequestTemplate(String name, Map<XmlPath, Node> clientAttrPaths) {
        super(clientAttrPaths);
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
