package org.myprojects.srvrequestcontrolservice;

import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.myprojects.srvrequestcontrolservice.utils.XmlUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.FileCopyUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
public class XmlUtilsTest {

    @Value("classpath:request.xml")
    Resource requestFile;
    @Value("classpath:template.xml")
    Resource templateFile;

    String templateStr;
    String reqStr;

    @Before
    public void init() throws IOException {
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
    }

    @Test
    public void checkNodeIsTextValueNode() throws Exception {
        Document doc = XmlUtils.getDocumentFromXmlString(reqStr);
        Node parent = doc.getFirstChild();
        parent = XmlUtils.getElementNodesSorted(parent.getChildNodes()).stream()
                .findFirst().get();
        Node textValueChild = null;
        Node nonTextValueChild = null;
        NodeList nodes = parent.getChildNodes();
        for (Node n : XmlUtils.getElementNodesSorted(nodes)) {
            if (n.getNodeName().equals("main:ClientINN")) {
                textValueChild = n;
            }
            if (n.getNodeName().equals("main:ArrayOfSections")) {
                nonTextValueChild = n;
            }
        }
        Assertions.assertTrue(XmlUtils.nodeIsTextValueNode(textValueChild));
        Assertions.assertFalse(XmlUtils.nodeIsTextValueNode(nonTextValueChild));
    }

    @Test
    public void checkNodeHasClientAttributeMark() throws Exception {
        Document doc = XmlUtils.getDocumentFromXmlString(templateStr);
        Node parent = doc.getFirstChild();
        parent = XmlUtils.getElementNodesSorted(parent.getChildNodes()).stream()
                .filter(e -> e.getNodeType() == Node.ELEMENT_NODE)
                .findFirst().get();
        Node clientAttrMarkChild = null;
        NodeList nodes = parent.getChildNodes();
        for (Node n : XmlUtils.getElementNodesSorted(nodes)) {
            if (n.getNodeName().equals("main:ClientTypeId")) {
                clientAttrMarkChild = n;
            }
        }

        Assertions.assertTrue(XmlUtils.nodeHasClientAttributeMark(clientAttrMarkChild));
    }

    @Test
    public void checkNodeHasPathMark() throws Exception {
        Document doc = XmlUtils.getDocumentFromXmlString(templateStr);
        Node parent = doc.getFirstChild();
        parent = XmlUtils.getElementNodesSorted(parent.getChildNodes()).stream()
                .filter(e -> e.getNodeType() == Node.ELEMENT_NODE)
                .findFirst().get();
        Node pathMarkChild = null;
        NodeList nodes = parent.getChildNodes();
        for (Node n : XmlUtils.getElementNodesSorted(nodes)) {
            if (n.getNodeName().equals("main:RequestTypeId")) {
                pathMarkChild = n;
            }
        }

        Assertions.assertTrue(XmlUtils.nodeHasPathMark(pathMarkChild));
    }

    @Test
    public void checkNodeIsDynamicPath() throws Exception {
        XmlPathNode n1 = new XmlPathNode(XmlPathNode.Type.NODE, "tem:MainRequest");
        List<XmlPathNode> children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:RequestTypeId", "1"));
        XmlPathNode n2 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "tem:Request", children);
        XmlPathNode n3 = new XmlPathNode(XmlPathNode.Type.NODE, "main:Attributes");

        List<XmlPathNode> children1 = new ArrayList<>();
        children1.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE_DYNAMIC, "main:SectionNumber"));
        children1.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:SectionId", "6"));
        XmlPathNode n4 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:Section", children1);

        XmlPathNode n5 = new XmlPathNode(XmlPathNode.Type.NODE, "main:Attributes");

        List<XmlPathNode> children2 = new ArrayList<>();
        children2.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:Name", "operationYear"));
        XmlPathNode n6 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:SectionAttribute", children2);

        XmlPath path = new XmlPath(List.of(n1, n2, n3, n4, n5, n6));

        Assertions.assertTrue(XmlUtils.isDynamicPath(path));
    }

    @Test
    public void checkGetTemplateNearestDynamicNode_isFound() throws Exception {
        Document templateDoc = XmlUtils.getDocumentFromXmlString(templateStr);

        XmlPathNode n1 = new XmlPathNode(XmlPathNode.Type.NODE, "tem:MainRequest");
        List<XmlPathNode> children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:RequestTypeId", "1"));
        XmlPathNode n2 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "tem:Request", children);
        XmlPathNode n3 = new XmlPathNode(XmlPathNode.Type.NODE, "main:ArrayOfSections");

        children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:SectionId", "6"));
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE_DYNAMIC, "main:SectionNumber"));
        XmlPathNode n4 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:Section", children);

        XmlPathNode n5 = new XmlPathNode(XmlPathNode.Type.NODE, "main:SectionAttributes");

        children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:Name", "operationYear"));
        XmlPathNode n6 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:SectionAttribute", children);

        XmlPath path = new XmlPath(List.of(n1, n2, n3, n4, n5, n6));

        Map<XmlPath, Node> lastPathNodes = XmlUtils.findNodesMatchingPathFromParent(templateDoc, path);
        Node lastPathNode = lastPathNodes.get(path);

        Assertions.assertEquals("main:SectionNumber",
                XmlUtils.getNearestDynamicNodeChild(path, lastPathNode).getNodeName());
    }

    @Test
    public void checkCreateParentDynamicNodeByTemplatePathNode_dynamicValueAddedToPathNode() throws Exception {
        Document requestDoc = XmlUtils.getDocumentFromXmlString(reqStr);

        XmlPathNode n1 = new XmlPathNode(XmlPathNode.Type.NODE, "tem:MainRequest");
        List<XmlPathNode> children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:RequestTypeId", "1"));
        XmlPathNode n2 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "tem:Request", children);
        XmlPathNode n3 = new XmlPathNode(XmlPathNode.Type.NODE, "main:ArrayOfSections");

        children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:SectionId", "6"));
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE_DYNAMIC, "main:SectionNumber"));
        XmlPathNode n4 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:Section", children);
        XmlPath path = new XmlPath(List.of(n1, n2, n3, n4));

        Map<XmlPath, Node> pathNodes = XmlUtils.findNodesMatchingPathFromParent(requestDoc, path);

        children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:SectionId", "6"));
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE_DYNAMIC, "main:SectionNumber", "0"));
        XmlPathNode n4double = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:Section", children);
        XmlPath pathToNode = new XmlPath(List.of(n1, n2, n3, n4double));
        Node node = pathNodes.get(pathToNode);

        XmlPathNode pathNode = XmlUtils.createParentDynamicNodeByTemplatePathNode(node, n4);
        Assertions.assertTrue(pathNode.getChildNodes().stream()
                .anyMatch(pn -> pn.getName().equals("main:SectionNumber") && pn.getValue() != null));
    }

    @Test
    public void checkUpdateDynamicNodePathsIfSingleIsFalse_isNotUpdated() throws Exception {
        Document requestDoc = XmlUtils.getDocumentFromXmlString(reqStr);

        XmlPathNode n1 = new XmlPathNode(XmlPathNode.Type.NODE, "tem:MainRequest");
        List<XmlPathNode> children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:RequestTypeId", "1"));
        XmlPathNode n2 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "tem:Request", children);
        XmlPathNode n3 = new XmlPathNode(XmlPathNode.Type.NODE, "main:Attributes");

        children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:SectionId", "6"));
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE_DYNAMIC, "main:SectionNumber"));
        XmlPathNode n4 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:Section", children);
        children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:SectionId", "6"));
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE_DYNAMIC, "main:SectionNumber", "0"));
        XmlPathNode n4double1 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:Section", children);
        children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:SectionId", "6"));
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE_DYNAMIC, "main:SectionNumber", "1"));
        XmlPathNode n4double2 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:Section", children);

        XmlPathNode n5 = new XmlPathNode(XmlPathNode.Type.NODE, "main:Attributes");

        children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:Name", "operationYear"));
        XmlPathNode n6 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:SectionAttribute", children);

        XmlPath pathToNode = new XmlPath(List.of(n1, n2, n3, n4));
        Map<XmlPath, Node> pathNodes = XmlUtils.findNodesMatchingPathFromParent(requestDoc, pathToNode);

        Map<XmlPath, Node> pathNodesForUpdate = new HashMap<>();
        XmlPath path1 = new XmlPath(List.of(n4double1, n5, n6));
        XmlPath path2 = new XmlPath(List.of(n4double2, n5, n6));
        Node node1 = pathNodes.get(path1);
        Node node2 = pathNodes.get(path2);
        pathNodesForUpdate.put(path1, node1);
        pathNodesForUpdate.put(path2, node2);

        Map<XmlPath, Node> updatedPathNodes = XmlUtils.updateDynamicPathNodeIfSingle(n4, pathNodesForUpdate);
        String value = updatedPathNodes.keySet().stream()
                .findFirst().get().getPathElement(0).getChildNodes().get(1).getValue();
        Assertions.assertNotNull(value);
    }

    @Test
    public void checkFindNodesMatchingPathFromParent_allPathsPresent() throws Exception {
        Document requestDoc = XmlUtils.getDocumentFromXmlString(reqStr);

        XmlPathNode n1 = new XmlPathNode(XmlPathNode.Type.NODE, "tem:MainRequest");
        List<XmlPathNode> children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:RequestTypeId", "1"));
        XmlPathNode n2 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "tem:Request", children);
        XmlPathNode n3 = new XmlPathNode(XmlPathNode.Type.NODE, "main:ArrayOfSections");

        children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:SectionId", "6"));
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE_DYNAMIC, "main:SectionNumber"));
        XmlPathNode n4 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:Section", children);
        children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:SectionId", "6"));
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE_DYNAMIC, "main:SectionNumber", "0"));
        XmlPathNode n4double1 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:Section", children);
        children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:SectionId", "6"));
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE_DYNAMIC, "main:SectionNumber", "1"));
        XmlPathNode n4double2 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:Section", children);

        XmlPathNode n5 = new XmlPathNode(XmlPathNode.Type.NODE, "main:SectionAttributes");

        children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:Name", "operationYear"));
        XmlPathNode n6 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:SectionAttribute", children);

        XmlPath pathToNode = new XmlPath(List.of(n1, n2, n3, n4, n5, n6));
        Map<XmlPath, Node> pathNodes = XmlUtils.findNodesMatchingPathFromParent(requestDoc, pathToNode);

        XmlPath path1 = new XmlPath(List.of(n1, n2, n3, n4double1, n5, n6));
        XmlPath path2 = new XmlPath(List.of(n1, n2, n3, n4double2, n5, n6));
        Node node1 = pathNodes.get(path1);
        Node node2 = pathNodes.get(path2);

        Assertions.assertNotNull(node1);
        Assertions.assertNotNull(node2);
    }

    @Test
    public void checkPathNodeCount() throws Exception {
        Document doc = XmlUtils.getDocumentFromXmlString(templateStr);
        Node parent = doc.getFirstChild();
        parent = XmlUtils.getElementNodesSorted(parent.getChildNodes()).stream()
                .filter(e -> e.getNodeType() == Node.ELEMENT_NODE)
                .findFirst().get();
        NodeList nodes = parent.getChildNodes();

        List<Node> childList = XmlUtils.getPathChildList(XmlUtils.getElementNodesSorted(nodes));
        Assertions.assertEquals(1, childList.size());
    }

    @Test
    public void checkClientAttrNodeErrorDescription() throws Exception {
        Document doc = XmlUtils.getDocumentFromXmlString(templateStr);
        Node parent = doc.getFirstChild();
        parent = XmlUtils.getElementNodesSorted(parent.getChildNodes()).stream()
                .filter(e -> e.getNodeType() == Node.ELEMENT_NODE)
                .findFirst().get();
        NodeList nodes = parent.getChildNodes();
        Node pathMarkChild = null;
        for (Node n : XmlUtils.getElementNodesSorted(nodes)) {
            if (n.getNodeName().equals("main:RegionId")) {
                pathMarkChild = n;
            }
        }

        Assertions.assertEquals("RegionId description", XmlUtils.getErrorDescription(pathMarkChild));
    }

    @Test
    public void checkGetChildNodesByName() throws Exception {
        Document doc = XmlUtils.getDocumentFromXmlString(templateStr);
        Node parent = doc.getFirstChild();
        parent = XmlUtils.getElementNodesSorted(parent.getChildNodes()).stream()
                .filter(e -> e.getNodeType() == Node.ELEMENT_NODE)
                .findFirst().get();
        NodeList nodes = parent.getChildNodes();

        Assertions.assertEquals(1, XmlUtils.getChildNodesByName(
                XmlUtils.getElementNodesSorted(nodes), "main:AttachedFiles").size());
    }

    @Test
    public void checkGetChildTextNodeByNameAndTextValue() throws Exception {
        Document doc = XmlUtils.getDocumentFromXmlString(reqStr);
        Node parent = doc.getFirstChild();
        parent = XmlUtils.getElementNodesSorted(parent.getChildNodes()).stream()
                .filter(e -> e.getNodeType() == Node.ELEMENT_NODE)
                .findFirst().get();
        NodeList nodes = parent.getChildNodes();

        Assertions.assertNotNull(XmlUtils.getChildNodeByNameAndTextValue(
                XmlUtils.getElementNodesSorted(nodes), "main:FilialId", "FFGG"));
    }

    @Test
    public void givenSimpleTemplatePath_requestPathIsFound() throws Exception {
        Document doc = XmlUtils.getDocumentFromXmlString(templateStr);
        Map<XmlPath, Node> paths = XmlUtils.getTemplatePathsFromNode(new XmlPath(), doc.getFirstChild());

        XmlPathNode n1 = new XmlPathNode(XmlPathNode.Type.NODE, "tem:MainRequest");
        List<XmlPathNode> children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:RequestTypeId", "1"));
        XmlPathNode n2 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "tem:Request", children);
        XmlPathNode n3 = new XmlPathNode(XmlPathNode.Type.NODE, "main:ClientINN");

        XmlPath path = new XmlPath(List.of(n1, n2, n3));
        Assertions.assertNotNull(paths.get(path));
    }

    @Test
    public void givenTemplatePathWithDynamicNode_requestPathsAreFound() throws Exception {
        Document doc = XmlUtils.getDocumentFromXmlString(templateStr);
        Map<XmlPath, Node> paths = XmlUtils.getTemplatePathsFromNode(new XmlPath(), doc.getFirstChild());

        XmlPathNode n1 = new XmlPathNode(XmlPathNode.Type.NODE, "tem:MainRequest");
        List<XmlPathNode> children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:RequestTypeId", "1"));
        XmlPathNode n2 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "tem:Request", children);
        XmlPathNode n3 = new XmlPathNode(XmlPathNode.Type.NODE, "main:ArrayOfSections");

        List<XmlPathNode> children1 = new ArrayList<>();
        children1.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:SectionId", "6"));
        children1.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE_DYNAMIC, "main:SectionNumber"));
        XmlPathNode n4 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:Section", children1);

        XmlPathNode n5 = new XmlPathNode(XmlPathNode.Type.NODE, "main:SectionAttributes");

        List<XmlPathNode> children2 = new ArrayList<>();
        children2.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:Name", "operationYear"));
        XmlPathNode n6 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:SectionAttribute", children2);

        XmlPathNode n7 = new XmlPathNode(XmlPathNode.Type.NODE, "main:Value");

        XmlPath path = new XmlPath(List.of(n1, n2, n3, n4, n5, n6, n7));

        Assertions.assertNotNull(paths.get(path));
    }

    @Test
    public void givenSeveralNodesWithDynamicPath_allPathsAreFound() throws Exception {
        Document doc = XmlUtils.getDocumentFromXmlString(reqStr);

        XmlPathNode n1 = new XmlPathNode(XmlPathNode.Type.NODE, "tem:MainRequest");
        List<XmlPathNode> children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:RequestTypeId", "1"));
        XmlPathNode n2 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "tem:Request", children);
        XmlPathNode n3 = new XmlPathNode(XmlPathNode.Type.NODE, "main:ArrayOfSections");

        List<XmlPathNode> children1 = new ArrayList<>();
        children1.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:SectionId", "6"));
        children1.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE_DYNAMIC, "main:SectionNumber"));
        XmlPathNode n4 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:Section", children1);

        XmlPath path = new XmlPath(List.of(n1, n2, n3, n4));

        Map<XmlPath, Node> paths = XmlUtils.findNodesMatchingPathFromParent(doc, path);

        Assertions.assertEquals(2, paths.size());
    }

    @Test
    public void givenBackwardPath_parentNodeIsFound() throws Exception {
        Document doc = XmlUtils.getDocumentFromXmlString(reqStr);

        XmlPathNode n1 = new XmlPathNode(XmlPathNode.Type.NODE, "tem:MainRequest");
        List<XmlPathNode> children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:RequestTypeId", "1"));
        XmlPathNode n2 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "tem:Request", children);
        XmlPathNode n3 = new XmlPathNode(XmlPathNode.Type.NODE, "main:ArrayOfSections");

        List<XmlPathNode> children1 = new ArrayList<>();
        children1.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:SectionId", "1"));
        XmlPathNode n4 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:Section", children1);
        XmlPath path = new XmlPath(List.of(n1, n2, n3, n4));

        Map<XmlPath, Node> paths = XmlUtils.findNodesMatchingPathFromParent(doc, path);
        Node startNode = paths.get(path);

        XmlPath backPath = new XmlPath(List.of(n4, n3, n2, n1));

        Node parent = doc.getFirstChild();
        Node found = XmlUtils.findParentNodeByBackwardPath(startNode, backPath);

        Assertions.assertEquals(parent, found);
    }

    @Test
    public void givenTwoDifferentPath_childNodesFoundByDiff() throws Exception {
        Document doc = XmlUtils.getDocumentFromXmlString(reqStr);

        XmlPathNode n1 = new XmlPathNode(XmlPathNode.Type.NODE, "tem:MainRequest");
        List<XmlPathNode> children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:RequestTypeId", "1"));
        XmlPathNode n2 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "tem:Request", children);
        XmlPathNode n3 = new XmlPathNode(XmlPathNode.Type.NODE, "main:ArrayOfSections");

        List<XmlPathNode> children1 = new ArrayList<>();
        children1.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:SectionId", "1"));
        XmlPathNode n4 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:Section", children1);

        List<XmlPathNode> children2 = new ArrayList<>();
        children2.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:SectionId", "10"));
        XmlPathNode n5 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:Section", children2);
        XmlPath oldPath = new XmlPath(List.of(n1, n2, n3, n4));
        XmlPath newPath = new XmlPath(List.of(n1, n2, n3, n5));

        Map<XmlPath, Node> oldPaths = XmlUtils.findNodesMatchingPathFromParent(doc, oldPath);
        Node oldPathLastNode = oldPaths.get(oldPath);

        Map<XmlPath, Node> newPaths = XmlUtils.findNodesMatchingPathFromParent(doc, newPath);
        Node newPathLastNode = newPaths.get(newPath);

        Assertions.assertEquals(newPathLastNode, XmlUtils.getChildNodesByPathsDiff(
                oldPath, newPath, oldPathLastNode).get(newPath));
    }

    @Test
    public void givenTemplateAndRequestEqualNodes_matchNodesAreFound() throws Exception {
        Document reqDoc = XmlUtils.getDocumentFromXmlString(reqStr);
        Document templateDoc = XmlUtils.getDocumentFromXmlString(templateStr);

        XmlPathNode n1 = new XmlPathNode(XmlPathNode.Type.NODE, "tem:MainRequest");
        List<XmlPathNode> children = new ArrayList<>();
        children.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:RequestTypeId", "1"));
        XmlPathNode n2 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "tem:Request", children);
        XmlPathNode n3 = new XmlPathNode(XmlPathNode.Type.NODE, "main:ArrayOfSections");

        List<XmlPathNode> children1 = new ArrayList<>();
        children1.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:SectionId", "1"));
        XmlPathNode n4 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:Section", children1);

        XmlPathNode n5 = new XmlPathNode(XmlPathNode.Type.NODE, "main:SectionAttributes");

        List<XmlPathNode> children2 = new ArrayList<>();
        children2.add(new XmlPathNode(XmlPathNode.Type.CHILD_NODE, "main:Name", "ContractTransferType"));
        XmlPathNode n6 = new XmlPathNode(XmlPathNode.Type.NODE_WITH_CHILDREN, "main:SectionAttribute", children2);

        XmlPath path = new XmlPath(List.of(n1, n2, n3, n4, n5, n6));

        Map<XmlPath, Node> reqPaths = XmlUtils.findNodesMatchingPathFromParent(reqDoc, path);

        Map<XmlPath, Node> templatePaths = XmlUtils.findNodesMatchingPathFromParent(templateDoc, path);

        List<Node> matchNodes = XmlUtils.matchNodesByName(
                XmlUtils.getElementNodesSorted(reqPaths.get(path).getChildNodes()),
                XmlUtils.getElementNodesSorted(templatePaths.get(path).getChildNodes()));
        Assertions.assertEquals(2, matchNodes.size());
    }
}
