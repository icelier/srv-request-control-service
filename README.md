Сервис выполняет бизнес-проверки заявлений(обращений) клиента.

Заявление может быть создано со стороны Мастер-Системы либо системы одного из Филиалов(территориально разное расположение).
Кроме того есть Мастер-Система бизнес-проверок, которая находится на стороне Мастер-системы.
Обращения, поданные из Мастер-Системы синхронизируются в систему Филиала.
Обращения, поданные из системы Филиала, синхронизируются в Мастер-Систему.
Обращения передаются в xml-формате.

Проверки включают:
1) проверку версии обращения по последней сохраненной в БД версии (любое изменение в обращении должно быть оформлено новой версией)

2) проверку идентификаторов обращения по последним сохраненным в БД идентификаторам (идентификаторы не могут меняться)
У обращения есть следующие идентификаторы :\
        - ID_FILIAL(идентификатор обращения Филиала),\
        - ID_MASTER_SYSTEM(идентификатор обращения Мастер-Системы),\
        - ID_INTEGRATION(идентификатор интеграции Мастер-Ситемы и Филиала),\
        - ID_MAIN_CHECK_SYSTEM(идентификатор обращения Мастер-Системы бизнес-проверок),\
        - FILIAL_ID(идентификатор самого Филиала),\
        - REQUEST_TYPE_ID(тип обращения);

В обращении всегда есть идентификатор ID_INTEGRATION, FILIAL_ID и либо ID_FILIAL, либо ID_MASTER_SYSTEM, в зависимости от того, где инициировано обращение (могут присутствовать оба одновременно).

3) проверку неизменности клиентских атрибутов 
Клиентские атрибуты - используются только для обращений, первая версия которых были подана с Мастер-системы.
Но после синхронизации с системой Филиала, последняя может оформить новую версию обращения с какими-то изменениями.
Клиентские атрибуты - те, которые система Филиала не может менять для таких обращений
Проверка выполняется по последнему сохраненному в БД обращении, полученному с Мастер-системы 

Поскольку перечень клиентских атрибутов постоянно обновляется, то для проверки того, что клиентские атрибуты не изменились, обращение, полученное от Мастер-системы, сохраняется в бд в виде строки оригинального xml-сообщения, также в бд хранится xsd-схема структуры данных (далее - шаблон), в которой атрибутами xml-тегов помечены клиентские атрибуты.

Есть атрибуты xml-тегов, указывающие на персональные данные. Значения таких тегов хранятся в БД после хеширования по md5.

Структура данных обращения представляет собой атрибуты уровня обращения(например, ClientEmail), затем вложенные теги секций(повторяющийся тег Section), внутри которых вложены теги атрибутов этих секций (повторяющийся тег SectionAttribute).
Также в обращении есть файлы уровня обращения(тег AttachedFiles) и файлы, вложенные в секции(тег AttachedFiles внутри тега Section).

Проверка неизменности клиентских атрибутов
При поступлении обращения со стороны системы Филиала, xml-структура поступившего запроса сравнивается с последним сохраненным в БД обращением от Мастер-системы. 

Строковые значения сравниваются посимвольно, в случае если значение атрибута - числовое, сравниваются без учета лишних нулей в дробной части.

Шаблоны также хранятся в БД, и кэшируются внутри сервиса. При обнаружении нового шаблона кэш обновляется.

Шаблон и обе заявки (сохраненная и проверяемая) из строкового представляения преобразовываются в DOM-дерево.
По меткам клиентских атрибутов из шаблона формируются пути к клиентским атрибутам в виде мапы (ключ-путь к тегу, хранящему клиентский атрибут, значение - тег(Node) с этим клиентским атрибутом), далее эти пути находятся в сохраненном и проверяемом обращении.
Путь к тегу с клиенстким атрибутом формируется из тегов xml-дерева, некоторые из этих тегов могут иметь уникальное значение (например, ClientEmail), либо потворяющееся (повторяющийся тег Section).
Повторяющиесся теги могут быть частью пути, но для этого они должны иметь вложенные теги с фиксированным значением (чтобы отличать от других таких же тегов), но также есть теги, которые являются частью пути к тегу с клиенстким атрибутом, но могут иметь динамическое значение.

Пример пути к тегу с клиентским атрибутом:
```
<main:ClientINN>{значение клиентского атрибута берется отсюда}</main:ClientINN> 
```
ClientINN - уникальный (неповторяющийся) тег уровня обращения(не секции)

Пример пути к тегу с клиентским атрибутом через повторяющийся тег атрибута секции с вложенными тегами имени и значения, частью пути является тег SectionAttribute с вложенным тегом Name с конкретным значением = TypeTC:
```
<SectionAttribute>
  <Name>TypeTC</Name>
  <Value>{значение клиентского атрибута берется отсюда}<Value/>
</SectionAttribute>
```
Также частью пути может быть тег с динамическим значением - используется для так называемых множественных секций.
Множественные секции - тег Section с тегом SectionId с фиксированным значением и с тегом SectionNumber с динамическим значением (номер по счету от 0 до бесконечности).
Т.о. за счет комбинации фиксированного SectionId и переменного SectionNumber получаются "множественные" секции.
В этом случае путь к Node с клиентским атрибутом внутри "множественной" секции невозможно указать полностью, поскольку SectionNumber определяется по конкретным пришедшим данным, поэтому в шаблоне указываются без конкретного значения.
Если в обращении Мастер-системы множественная секция была одна, то переменное значение тега SectionNumber не учитывается при сравнении проверяемого и сохраненного обращений. 
Если больше одной, то для возможности проверки SectionNumber указываются при формировании путей проверяемого и сохраненного обращений. 
По совпадению путей и значений в Node по каждому пути проверяется, поменялись клиентские атрибуты или нет.
Если Node с клиентским атрибутом не пришла в проверяемом обращении, считается, что она не меняется, значит ошибки нет. 
