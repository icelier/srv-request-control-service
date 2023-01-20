package org.myprojects.srvrequestcontrolservice;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class XmlPathNode {

    private final Type type;
    protected String name;
    protected String value;
    protected List<XmlPathNode> childNodes;

    public String getValue() {
        return value;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public List<XmlPathNode> getChildNodes() {
        if (childNodes != null) {
            return Collections.unmodifiableList(childNodes);
        } else {
            return Collections.emptyList();
        }
    }

    public XmlPathNode(Type type, String name) {
        this.type = type;
        this.name = name;
    }

    public XmlPathNode(Type type, String name, String value) {
        this(type, name);
        this.value = value;
    }

    public XmlPathNode(Type type, String name, List<XmlPathNode> childNodes) {
        this(type, name);
        this.childNodes = childNodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XmlPathNode pathNode = (XmlPathNode) o;
        return type == pathNode.type && name.equals(pathNode.name)
                && Objects.equals(value, pathNode.value)
                && Objects.equals(childNodes, pathNode.childNodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name, value, childNodes);
    }

    public enum Type {
        NODE,
        NODE_WITH_CHILDREN,
        CHILD_NODE,
        CHILD_NODE_DYNAMIC
    }
}
