package org.myprojects.srvrequestcontrolservice;

import org.w3c.dom.Node;

import java.util.Map;

public abstract class XmlRequestPaths {

    protected Map<XmlPath, Node> paths;

    protected XmlRequestPaths(Map<XmlPath, Node> paths) {
        this.paths = paths;
    }

    public Map<XmlPath, Node> getPaths() {
        return paths;
    }
}
