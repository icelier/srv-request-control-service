package org.myprojects.srvrequestcontrolservice.data;

public class RequestIdentifier {

    private Id idName;
    private String idValue;

    public RequestIdentifier(Id idName, String idValue) {
        this.idName = idName;
        this.idValue = idValue;
    }

    public Id getIdName() {
        return idName;
    }

    public String getIdValue() {
        return idValue;
    }

    public enum Id {
        ID_INTEGRATION("id_integration"),
        ID_FILIAL("id_filial"),
        ID_MASTER_SYSTEM("id_master_system"),
        ID_MAIN_CHECK_SYSTEM("id_main_check_system"),
        FILIAL_ID("filial_id"),
        REQUEST_TYPE_ID("request_type_id");

        private String value;

        Id(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
