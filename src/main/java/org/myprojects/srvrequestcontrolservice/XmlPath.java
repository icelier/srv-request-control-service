package org.myprojects.srvrequestcontrolservice;

import java.util.*;

public class XmlPath {

    private List<XmlPathNode> pathElements = new ArrayList<>();

    public XmlPath() {}

    public XmlPath(Collection<XmlPathNode> pathNodes) {
        this.pathElements.addAll(pathNodes);
    }

    public XmlPath(XmlPathNode... pathNodes) {
        this.pathElements.addAll(Arrays.asList(pathNodes));
    }

    public XmlPath(XmlPath other) {
        addAllElements(other);
    }

    public void addPathElement(XmlPathNode pathElement) {
        pathElements.add(pathElement);
    }

    public List<XmlPathNode> getPathElements() {
        return Collections.unmodifiableList(pathElements);
    }

    public List<XmlPathNode> getPathElementsSubList(int fromIndex, int toIndex) {
        return pathElements.subList(fromIndex, toIndex);
    }

    public List<XmlPathNode> getPathElementsSubList(int fromIndex) {
        return getPathElementsSubList(fromIndex, this.getPathElementsLength());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XmlPath xmlPath = (XmlPath) o;
        return pathElements.equals(xmlPath.pathElements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pathElements);
    }

    public void addAllElements(XmlPath otherPath) {
        for (XmlPathNode el : otherPath.getPathElements()) {
            this.addPathElement(el);
        }
    }

    public void addAllElements(List<XmlPathNode> pathNodes) {
        for (XmlPathNode el : pathNodes) {
            this.addPathElement(el);
        }
    }

    public int containsPath(XmlPath otherPath, int index) {
        if (index >= this.pathElements.size()) {
            return -1;
        }

        int lastMatchIndex = -1;
        int length = Math.min(this.getPathElementsLength(), otherPath.getPathElementsLength());
        for (int i = index; i < length; i++) {
            if (this.pathElements.get(i).equals(otherPath.getPathElement(i))) {
                lastMatchIndex = i;
            } else {
                break;
            }
        }

        return lastMatchIndex;
    }

    public XmlPathNode getPathElement(int index) {
        if (index >= pathElements.size()) {
            return null;
        }

        return pathElements.get(index);
    }

    public int getPathElementsLength() {
        return this.pathElements.size();
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for(XmlPathNode pathNode : pathElements) {
            result.append(pathNode.getName()).append("/");
            for (XmlPathNode childNode : pathNode.getChildNodes()) {
                result.append(childNode.getName()).append("=").append(childNode.getValue()).append("/");
            }
        }
        return result.toString();
    }
}
