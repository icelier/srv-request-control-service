package org.myprojects.srvrequestcontrolservice.utils;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.myprojects.srvrequestcontrolservice.XmlPath;
import org.myprojects.srvrequestcontrolservice.XmlPathNode;
import org.myprojects.srvrequestcontrolservice.exceptions.ClientAttributesDataException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static org.myprojects.srvrequestcontrolservice.ClientAttributesServiceOperator.ClientAttributesConstants.*;
import static org.myprojects.srvrequestcontrolservice.XmlPathNode.Type.*;

public class XmlUtils {

    public static final String CLIENT_ATTR_VALUE_REG_EX = "[,\"'( )]*";
    public static final String HASH = "hash";
    public static final String KEY = "key";
    public static final String SEGMENT = "filial";

    private XmlUtils() {}

    public static Document getPreparedDocumentFromXmlString(String xmlString)
            throws ParserConfigurationException, IOException, SAXException {
        byte[] byteArray = xmlString.getBytes(StandardCharsets.UTF_8);

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        Document doc = factory
                .newDocumentBuilder()
                .parse(new ByteArrayInputStream(byteArray));
        // для text nodes
        doc.normalizeDocument();

        // убираем лишние символы не влияющие на изменение содержания (пробелы, кавычки, скобки...)
        replaceTextValueIrrelevantSymbols(doc);

        return doc;
    }

    public static Document getDocumentFromXmlString(String xmlString)
            throws ParserConfigurationException, IOException, SAXException {
        byte[] byteArray = xmlString.getBytes(StandardCharsets.UTF_8);

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        Document doc = factory
                .newDocumentBuilder()
                .parse(new ByteArrayInputStream(byteArray));
        // для text nodes
        doc.normalizeDocument();

        return doc;
    }

    public static String getXmlStringFromDocument(Document doc) throws TransformerException {
        DOMSource in = new DOMSource(doc);
        StreamResult out = new StreamResult(new StringWriter());

        TransformerFactory factory = TransformerFactory.newInstance();
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
        Transformer transformer = factory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.transform(in, out);

        return out.getWriter().toString();
    }

    public static Map<XmlPath, Node> getTemplatePathsFromNode(XmlPath pathToParent, Node parent) {
        int parentNodeType = parent.getNodeType();
        if (parentNodeType != Node.ELEMENT_NODE) {
            return Collections.emptyMap();
        }

        XmlPath currentPath = new XmlPath(pathToParent);
        // формируем мапу, где ключом является путь к нодам, хранящим текстовое значение клиентского атрибута
        Map<XmlPath, Node> nodePaths = new HashMap<>();

        List<Node> childNodes = getElementNodesSorted(parent.getChildNodes());
        // выбираем чилды, которые содержат отметку путей до клиентских атрибутов
        List<Node> pathNodes = getPathChildList(childNodes);
        // выбираем из чилдов те, которые содержат отметку клиентского атрибута
        List<Node> clientAttrNodes = childNodes.stream()
                .filter(XmlUtils::nodeHasClientAttributeMark)
                .collect(Collectors.toList());

        // добавляем парент ноду и ее чилды с текстовым значением (если есть) в путь
        if (!pathNodes.isEmpty()) {
            List<XmlPathNode> children = new ArrayList<>();
            for (Node pathNode : pathNodes) {
                XmlPathNode xmlPathNode;
                // в случае НЕдинамических нод (обычных)
                if (XmlUtils.nodeIsTextValueNode(pathNode)) {
                    xmlPathNode = new XmlPathNode(CHILD_NODE, pathNode.getNodeName(),
                            pathNode.getFirstChild().getNodeValue());
                } else {
                    // в случае динамических нод (т е текстовое значение такой ноды может быть динамическое,
                    // а не фиксированное)
                    xmlPathNode = new XmlPathNode(CHILD_NODE_DYNAMIC, pathNode.getNodeName());
                }
                children.add(xmlPathNode);
            }
            currentPath.addPathElement(new XmlPathNode(NODE_WITH_CHILDREN, parent.getNodeName(),
                    children));
        } else {
            // если нет чилдов, являющихся частью пути, добавляем только саму ноду в путь
            currentPath.addPathElement(new XmlPathNode(NODE, parent.getNodeName()));
        }
        // удаляем обработанные чилды для дальнейшей обработки остальных чилдов
        childNodes.removeAll(pathNodes);

        // проверяем чилды, в которых стоит отметка клиентского атрибута,
        // и добавляем к пути
        for (Node clientAttrNode : clientAttrNodes) {
            XmlPath nodePath = new XmlPath(currentPath);
            nodePath.addPathElement(new XmlPathNode(NODE, clientAttrNode.getNodeName()));
            nodePaths.put(nodePath, clientAttrNode);
        }
        // удаляем обработанные чилды с клиентским атрибутом для дальнейшей обработки остальных чилдов
        childNodes.removeAll(clientAttrNodes);

        // проверяем остальные чилды, не являющиеся текстовыми чилдами (дальше по дереву вниз)
        for (Node otherNode : childNodes) {
            // рекурсивно получаем пути для чилдов с иерархией
            nodePaths.putAll(getTemplatePathsFromNode(currentPath, otherNode));
        }

        return nodePaths;
    }

    public static List<Node> getElementNodesSorted(NodeList nodes) {
        List<Node> nodeList = new ArrayList<>();

        for (int i = 0, length = nodes.getLength(); i < length; i++) {
            nodeList.add(nodes.item(i));
        }

        return nodeList.stream()
                .filter(n -> n.getNodeType() == Node.ELEMENT_NODE)
                // сортируем для того, чтобы при каждой обработке ноды выстраивались в одном и том же порядке
                // для разных запросов
                .sorted(Comparator.comparing(Node::getNodeName)
                        .thenComparing(Node::getNodeValue, Comparator.nullsLast(String::compareTo)))
                .collect(Collectors.toList());
    }

    public static List<Node> getAllNodesAsList(NodeList nodes) {
        List<Node> nodeList = new ArrayList<>();

        for (int i = 0, length = nodes.getLength(); i < length; i++) {
            nodeList.add(nodes.item(i));
        }

        return nodeList;
    }

    public static List<Node> getPathChildList(List<Node> childNodes) {
        return childNodes.stream()
                .filter(XmlUtils::nodeHasPathMark)
                .collect(Collectors.toList());
    }

    public static Node getNearestDynamicNodeChild(XmlPath templatePath, Node requestLastNodeByPath)  {
        XmlPathNode templatePathNode;
        int dynamicNodeIndex = -1;
        String dynamicChildName = null;
        // ищем индекс ближайшей от конца xml-дерева динамической ноды
        for (int i = templatePath.getPathElementsLength() - 1; i >= 0; i--) {
            templatePathNode = templatePath.getPathElement(i);
            if (templatePathNode.getType() == NODE_WITH_CHILDREN) {
                for (XmlPathNode child : templatePathNode.getChildNodes()) {
                    // TODO - метод предусматривает, что динамических чилдов мождет быть только один
                    // для других случаев нужна будет доработка (сейчас не требуется)
                    if (child.getType() == CHILD_NODE_DYNAMIC) {
                        dynamicNodeIndex = i;
                        dynamicChildName = child.getName();
                        break;
                    }
                }
                if (dynamicNodeIndex != -1) {
                    break;
                }
            }
        }
        if (dynamicNodeIndex == -1) {
            return null;
        }

        // получаем обратный путь с конца xml-дерева запроса к динамической ноде
        List<XmlPathNode> subPath = new ArrayList<>(templatePath.getPathElementsSubList(dynamicNodeIndex));
        Collections.reverse(subPath);

        // находим парент ноду запроса по полученному пути
        Node dynamicParentNode = XmlUtils.findParentNodeByBackwardPath(requestLastNodeByPath, new XmlPath(subPath));

        // находим чилд парент ноды по имени
        return getChildNodeByName(dynamicParentNode, dynamicChildName);
    }

    public static Node getChildNodeByName(Node parentNode, String childName) {
        for (Node child : XmlUtils.getElementNodesSorted(parentNode.getChildNodes())) {
            if (child.getNodeName().equals(childName)) {
                return child;
            }
        }

        return null;
    }

    public static boolean isDynamicPath(XmlPath path) {
        for (XmlPathNode pathNode : path.getPathElements()) {
            if (isParentPathNodeDynamic(pathNode)) {
                return true;
            }
        }

        return false;
    }

    public static boolean isParentPathNodeDynamic(XmlPathNode pathNode) {
        if (pathNode.getType() == NODE_WITH_CHILDREN) {
            for (XmlPathNode child : pathNode.getChildNodes()) {
                if (child.getType() == CHILD_NODE_DYNAMIC) {
                    return true;
                }
            }
        }

        return false;
    }

    public static XmlPath createTemplateDynamicPath(XmlPath filledPath) {
        XmlPath templateDynamicPath = new XmlPath();

        for (XmlPathNode pathNode : filledPath.getPathElements()) {
            // если нода динамическая, заменяем ее новой нодой с динкамическим (пустым) значением
            if (isParentPathNodeDynamic(pathNode)) {
                List<XmlPathNode> children = new ArrayList<>();
                for (XmlPathNode child : pathNode.getChildNodes()) {
                    if (child.getType() == CHILD_NODE_DYNAMIC) {
                        // для динамичсеких чилдов создаем новый узел без конкретного значения
                        // и добавляем его в путь
                        XmlPathNode dynamicChild = new XmlPathNode(CHILD_NODE_DYNAMIC, child.getName());
                        children.add(dynamicChild);
                    } else {
                        children.add(child);
                    }
                }
                XmlPathNode dynamicNode = new XmlPathNode(NODE_WITH_CHILDREN, pathNode.getName(), children);

                templateDynamicPath.addPathElement(dynamicNode);
            } else {
                // если нода НЕдинамическая, оставляем как было
                templateDynamicPath.addPathElement(pathNode);
            }
        }

        return templateDynamicPath;
    }

    public static boolean nodeIsTextValueNode(Node node) {
        return node.getChildNodes() != null && node.getChildNodes().getLength() == 1
                && node.getChildNodes().item(0).getNodeType() == Node.TEXT_NODE
                && node.getChildNodes().item(0).getNodeValue() != null;
    }

    public static boolean nodeHasClientAttributeMark(Node node) {
        NamedNodeMap attributes = node.getAttributes();
        if (attributes == null) {
            return false;
        }
        for (int i = 0, length = attributes.getLength(); i < length; i++) {
            if (attributes.item(i).getNodeName().equals(CLIENT_ATTRIBUTE)
                    && attributes.item(i).getNodeValue().equals("true")) {
                return true;
            }
        }

        return false;
    }

    public static boolean nodeHasPathMark(Node node) {
        NamedNodeMap attributes = node.getAttributes();
        if (attributes == null) {
            return false;
        }
        for (int i = 0, length = attributes.getLength(); i < length; i++) {
            if (attributes.item(i).getNodeName().equals(PATH)
                    && attributes.item(i).getNodeValue().equals("true")) {
                return true;
            }
        }

        return false;
    }

    public static boolean nodeHasPersonalDataMark(Node node) {
        NamedNodeMap attributes = node.getAttributes();
        if (attributes == null) {
            return false;
        }
        for (int i = 0, length = attributes.getLength(); i < length; i++) {
            if (attributes.item(i).getNodeName().equals(PERSONAL_DATA)
                    && attributes.item(i).getNodeValue().equals("true")) {
                return true;
            }
        }

        return false;
    }

    public static void hashPersonalDataAttribute(Node node) {
        if (XmlUtils.nodeIsTextValueNode(node)) {
            String value = node.getFirstChild().getNodeValue();
            String hashedValue = DigestUtils.md5Hex(value);
            node.getFirstChild().setNodeValue(hashedValue);
        }
    }

    public static String getErrorDescription(Node node) {
        NamedNodeMap attributes = node.getAttributes();
        for (int i = 0, length = attributes.getLength(); i < length; i++) {
            if (attributes.item(i).getNodeName().equals(ERROR_DESCRIPTION)) {
                return attributes.item(i).getNodeValue();
            }
        }

        return null;
    }

    public static String getDynamicErrorDescription(XmlPath path, Node templateChildNode, Node reqChildNode)
            throws ClientAttributesDataException {
        Node templateDynamicNode = XmlUtils.getNearestDynamicNodeChild(path, templateChildNode);
        Node reqDynamicNode = XmlUtils.getNearestDynamicNodeChild(path, reqChildNode);
        if (templateDynamicNode == null || reqDynamicNode == null) {
            throw new IllegalArgumentException("Некорректная структура данных.");
        }

        String error = XmlUtils.getErrorDescription(templateChildNode);
        if (error == null) {
            throw new ClientAttributesDataException("Не найдено описание ошибки атрибута "
                    + templateChildNode.getNodeName());
        }

        String additionalDescription = XmlUtils.getPrefixDescription(templateDynamicNode);
        if (additionalDescription != null) {
            return additionalDescription + reqDynamicNode.getFirstChild().getNodeValue() + ". " + error;
        } else {
            additionalDescription = XmlUtils.getPostfixDescription(templateDynamicNode);
            if (additionalDescription == null) {
                throw new IllegalArgumentException("Для динамической ошибки не найдена переменная часть описания.");
            } else {
                return error +". " +  additionalDescription + reqDynamicNode.getFirstChild().getNodeValue();
            }
        }
    }

    public static String getPrefixDescription(Node node) {
        NamedNodeMap attributes = node.getAttributes();
        for (int i = 0, length = attributes.getLength(); i < length; i++) {
            if (attributes.item(i).getNodeName().equals(PREFIX_ERROR_DESCRIPTION)) {
                return attributes.item(i).getNodeValue();
            }
        }

        return null;
    }

    public static String getPostfixDescription(Node node) {
        NamedNodeMap attributes = node.getAttributes();
        for (int i = 0, length = attributes.getLength(); i < length; i++) {
            if (attributes.item(i).getNodeName().equals(POSTFIX_ERROR_DESCRIPTION)) {
                return attributes.item(i).getNodeValue();
            }
        }

        return null;
    }

    public static Map<XmlPath, Node> findNodesMatchingPathFromParent(Node parentNode, XmlPath templatePath) {
        // формируем все пути, которые получаем из текущей ноды
        Map<XmlPath, Node> nodePaths = new HashMap<>();
        // проходим по каждой ноде из пути
        XmlPathNode currentPathNode = templatePath.getPathElement(0);
        for (Node node : XmlUtils.getElementNodesSorted(parentNode.getChildNodes())) {
            // если имя ноды не совпадает с тем, которое ищем, пропускаем
            if (!node.getNodeName().equals(currentPathNode.getName())) {
                continue;
            }
            XmlPathNode firstPathNode = null;
            if (currentPathNode.getType() == NODE) {
                firstPathNode = currentPathNode;
            } else if (currentPathNode.getType() == NODE_WITH_CHILDREN) {
                if (XmlUtils.nodeHasChildNodesByChildPaths(XmlUtils.getElementNodesSorted(
                        node.getChildNodes()), currentPathNode.getChildNodes())) {
                    firstPathNode = currentPathNode;
                    // для динамического элемента пути формируем ноду пути с конкретным динамическим значением
                    if (XmlUtils.isParentPathNodeDynamic(currentPathNode)) {
                        firstPathNode = createParentDynamicNodeByTemplatePathNode(node, currentPathNode);
                    }
                }
            }
            if (firstPathNode != null) {
                if (templatePath.getPathElementsLength() == 1) {
                    nodePaths.put(new XmlPath(List.of(firstPathNode)), node);
                } else {
                    // рекурсивно ищем следующие элементы и формируем обобщенный путь
                    XmlPathNode finalParentPathNode = firstPathNode;
                    findNodesMatchingPathFromParent(node, new XmlPath(templatePath.getPathElementsSubList(1)))
                            .forEach((key, value) -> {
                                // создаем новый путь из текущей ноды
                                // и пути полученного из рекурсивного вызова для вложенных нод
                                XmlPath p = new XmlPath(finalParentPathNode);
                                p.addAllElements(key);
                                nodePaths.put(p, value);});
                }
            }
        }
        // для динамических нод (например, множественных секций) проверяем,
        // если такая множественная секция найдена одна, то формируем путь без учета динамического значения
        // (т.к. для сравнения не важно, с каким динамическим значением будет нода, если она одна)
        // если больше одной - путь формируется с конкретным динамическим значением (например, sectionNumber = 0, 1...)
        return updateDynamicPathNodeIfSingle(currentPathNode, nodePaths);
    }

    public static Map<XmlPath, Node> updateDynamicPathNodeIfSingle(XmlPathNode dynamicPathNode,
                                                                   Map<XmlPath, Node> nodePaths) {
        if (XmlUtils.isParentPathNodeDynamic(dynamicPathNode)) {
            XmlPathNode pathNode = null;
            boolean singleDynamicPath = true;
            for (Map.Entry<XmlPath, Node> e : nodePaths.entrySet()) {
                if (pathNode == null) {
                    pathNode = e.getKey().getPathElement(0);
                } else {
                    // исключаем дублирование множественной секции с одним и тем же sectionNumber
                    if (!e.getKey().getPathElement(0).equals(pathNode)) {
                        singleDynamicPath = false;
                    }
                }
            }

            // если обнаружена только одна динамическая нода для текущего динамического пути
            if (singleDynamicPath) {
                return nodePaths.entrySet().stream().collect(Collectors.toMap(e -> {
                    // подменяем в ней первую ноду, которая и есть динамическая,
                    // оставляя без конкретного динамического значения, т.к. оно не играет роли при сравнении
                    XmlPath newPath = new XmlPath(dynamicPathNode);
                    newPath.addAllElements(e.getKey().getPathElementsSubList(1));
                    return newPath;
                }, Map.Entry::getValue));
            } else {
                return nodePaths;
            }
        }

        return nodePaths;
    }

    public static Node findParentNodeByBackwardPath(Node pathLastNode, XmlPath backwardPath) {
        Node lastStepNode = pathLastNode;

        // проходим по переданной ноде вверх по дереву к искомому родителю из обратного пути (вверх по дереву)
        XmlPathNode el;
        for (int i = 0, length = backwardPath.getPathElementsLength(); i < length; i++) {
            el = backwardPath.getPathElement(i);
            // если один из элементов пути не найден, ноды с таким путем не существует
            if (lastStepNode == null) {
                return null;
            }
            switch (el.getType()) {
                case NODE:
                    if (!lastStepNode.getNodeName().equals(el.getName())) {
                        lastStepNode = null;
                    }
                    break;
                case NODE_WITH_CHILDREN:
                    if (!lastStepNode.getNodeName().equals(el.getName())
                            || !nodeHasChildNodesByChildPaths(getElementNodesSorted(lastStepNode.getChildNodes()), el.getChildNodes())) {
                        lastStepNode = null;
                    }
                    break;
                default:
                    lastStepNode = null;
                    break;
            }
            if (lastStepNode != null && i != length - 1) {
                lastStepNode = lastStepNode.getParentNode();
            }
        }

        return lastStepNode;
    }

    public static List<Node> getChildNodesByName(List<Node> nodeChildren, String nodeName) {
        List<Node> nodes = new ArrayList<>();
        for (Node child : nodeChildren) {
            if (child.getNodeName().equals(nodeName)) {
                nodes.add(child);
            }
        }

        return nodes;
    }

    public static Node getChildNodeByName(List<Node> nodeChildren, String nodeName) {
        for (Node child : nodeChildren) {
            if (child.getNodeName().equals(nodeName)) {
                return child;
            }
        }

        return null;
    }

    public static Node getChildNodeByNameAndTextValue(List<Node> nodeChildren, String nodeName,
                                                      String textValue) {
        for (Node child : nodeChildren) {
            if (nodeIsTextValueNode(child) && child.getNodeName().equals(nodeName)
                    && child.getFirstChild().getNodeValue().equals(textValue)) {
                return child;
            }
        }

        return null;
    }

    public static void replaceTextValueIrrelevantSymbols(Node parent) {
        List<Node> nodes = XmlUtils.getAllNodesAsList(parent.getChildNodes());

        // рекурсивно обрабатываем текстовые значения и заменяем ненужные символы,
        // чтобы не проверять их при сравнении сохраненного и проверяемого запросов
        for (Node node : nodes) {
            if (node.getNodeType() == Node.TEXT_NODE) {
                String value = node.getNodeValue();
                if (value != null && !value.isEmpty()) {
                    value = value.toLowerCase(Locale.ROOT).replaceAll(CLIENT_ATTR_VALUE_REG_EX, "");
                    // для файлов проверяем неизменность клиентского атрибута по хешу
                    if (value.contains(SEGMENT) && value.contains(KEY) && value.contains(HASH)) {
                        value = HASH + StringUtils.substringAfter(value, HASH);
                    }
                    node.setNodeValue(value);
                }
            } else {
                replaceTextValueIrrelevantSymbols(node);
            }
        }
    }

    public static String replaceTextValueIrrelevantSymbols(String value) {
        if (value != null && !value.isBlank()) {
            // для файлов проверяем неизменность клиентского атрибута по хешу
            if (value.contains(SEGMENT) && value.contains(KEY) && value.contains(HASH)) {
                value = HASH + StringUtils.substringAfter(value, HASH);
            }
        }

        return value == null ? null : value.trim();
    }

    public static List<Node> matchNodesByName(List<Node> templateNodes, List<Node> checkNodes) {
        List<Node> matchNodes = new ArrayList<>();

        for (Node templateNode : templateNodes) {
            matchNodes.addAll(getChildNodesByName(checkNodes, templateNode.getNodeName()));
        }

        return matchNodes;
    }

    public static XmlPathNode createParentDynamicNodeByTemplatePathNode(Node node, XmlPathNode pathNode) {
        Node match = null;
        List<XmlPathNode> children = new ArrayList<>();

        // проходим по всем чилдам динамического пути шаблона и создаем аналогичные чилды с конкретными значениями
        // для динамических элементов
        for (XmlPathNode childPathNode : pathNode.getChildNodes()) {
            XmlPathNode childNode = null;
            // найден динамический элемент пути шаблона
            if (childPathNode.getType() == CHILD_NODE_DYNAMIC) {
                match = getChildNodeByName(getElementNodesSorted(node.getChildNodes()),
                        childPathNode.getName());
                if (match != null) {
                    // если клиентский атрибут был передан с конкретным текстовым значением
                    if (XmlUtils.nodeIsTextValueNode(match)) {
                        childNode = new XmlPathNode(CHILD_NODE_DYNAMIC, match.getNodeName(),
                                match.getFirstChild().getNodeValue());
                    } else {
                        // если клиентский атрибут был передан без значения
                        childNode = new XmlPathNode(CHILD_NODE_DYNAMIC, match.getNodeName());
                    }
                }
            } else {
                // найден НЕдинамический элемент пути шаблона
                match = getChildNodeByNameAndTextValue(
                        getElementNodesSorted(node.getChildNodes()), childPathNode.getName(), childPathNode.getValue());
                if (match != null) {
                    childNode = new XmlPathNode(CHILD_NODE, match.getNodeName(), match.getFirstChild().getNodeValue());
                }
            }

            // если у ноды не было одного из чилдов при проверке
            if (childNode == null) {
                return null;
            }
            children.add(childNode);
        }

        return new XmlPathNode(NODE_WITH_CHILDREN, pathNode.getName(), children);
    }

    private static boolean nodeHasChildNodesByChildPaths(List<Node> childNodes, List<XmlPathNode> childPathNodes) {
        boolean hasChildNode = true;
        for (XmlPathNode childPathNode : childPathNodes) {
            if (!hasChildNode) {
                return false;
            }
            if (childPathNode.getValue() == null) {
                hasChildNode = !getChildNodesByName(childNodes, childPathNode.getName()).isEmpty();
            } else {
                hasChildNode = getChildNodeByNameAndTextValue(
                        childNodes, childPathNode.getName(), childPathNode.getValue()) != null;
            }
        }

        return hasChildNode;
    }

    public static Map<XmlPath, Node> getChildNodesByPathsDiff(XmlPath oldPath, XmlPath newPath, Node oldPathLastNode) {
        Map<XmlPath, Node> children = new HashMap<>();
        // ищем индекс последнего совпадающего элемента
        int subPathEndIndex = newPath.containsPath(oldPath, 0);
        // проверяем, что текущий путь содержит в начале предыдущий путь, чтобы не проходить повторно
        if (subPathEndIndex != -1) {
            // если есть совпадение, ищем ноду только по отличающейся части пути

            // если пути полностью совпадают, возвращаем текщую ноду для данного пути
            if (oldPath.getPathElementsLength() == newPath.getPathElementsLength()
                    && subPathEndIndex == newPath.getPathElementsLength() - 1) {
                children.put(newPath, oldPathLastNode);
                return children;
            }

            // если пути совпадают частично
            Node backParent = oldPathLastNode;

            // если необходимо вернуться назад по предыдущему пути, находим последнюю совпадающую ноду
            if (subPathEndIndex < oldPath.getPathElementsLength() - 1) {
                List<XmlPathNode> backShortPath = new ArrayList<>(oldPath.getPathElementsSubList(subPathEndIndex));
                Collections.reverse(backShortPath);
                backParent = XmlUtils.findParentNodeByBackwardPath(oldPathLastNode, new XmlPath(backShortPath));
            }

            // если поднимались вверх по дереву и новый путь был короче старого и оказались на последней ноде нового пути
            // возвращаем текщую ноду для данного пути
            if (subPathEndIndex == newPath.getPathElementsLength() - 1) {
                children.put(newPath, backParent);
                return children;
            }

            // ищем несовпадающую часть
            List<XmlPathNode> shortPathNodes = newPath.getPathElementsSubList(subPathEndIndex + 1);
            XmlPath shortPath = new XmlPath(shortPathNodes);
            children = XmlUtils.findNodesMatchingPathFromParent(backParent, shortPath);

            return children.entrySet().stream()
                    .collect(Collectors.toMap(e -> {
                        XmlPath fullPath = new XmlPath();
                        fullPath.addAllElements(newPath.getPathElementsSubList(0, subPathEndIndex + 1));
                        fullPath.addAllElements(shortPath);
                        return fullPath;
                    }, Map.Entry::getValue));
        }

        return children;
    }

    public static boolean compareClientAttrsValues(String savedValue, String checkValue) {
        // проверяем, числовые значения или нет
        try {
            double doubleSavedValue = Double.parseDouble(savedValue.trim());
            BigDecimal savedBigDecimal = new BigDecimal(String.valueOf(doubleSavedValue));

            double doubleCheckValue = Double.parseDouble(checkValue.trim());
            BigDecimal checkBigDecimal = new BigDecimal(String.valueOf(doubleCheckValue));

            // сравниваем числа без учета нулей
            return savedBigDecimal.compareTo(checkBigDecimal) == 0;

        } catch (NumberFormatException e) {
            return checkValue
                    .equals(savedValue);
        }
    }

    public static String checkFileAttribute(String value) {
        // для файлов проверяем неизменность клиентского атрибута по хешу
        if (value.contains(SEGMENT) && value.contains(KEY) && value.contains(HASH)) {
            value = HASH + StringUtils.substringAfter(value, HASH);
        }

        return value;
    }

    public static boolean checkIsZeroNumberAttribute(Node pathNode) {
        try {
            double value = Double.parseDouble(pathNode.getFirstChild().getNodeValue().trim());

            return new BigDecimal(String.valueOf(value)).compareTo(BigDecimal.ZERO) == 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
