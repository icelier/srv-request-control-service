package org.myprojects.srvrequestcontrolservice.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IdList {

    private List<RequestIdentifier> requestIds = new ArrayList<>();

    public IdList() {}

    public IdList(List<RequestIdentifier> requestIds) {
        this.requestIds = requestIds;
    }

    public List<RequestIdentifier> getRequestIds() {
        return Collections.unmodifiableList(requestIds);
    }

    public boolean contains(RequestIdentifier.Id identifier) {
        for (RequestIdentifier id : this.requestIds) {
            if (id.getIdName() == identifier) {
                return true;
            }
        }

        return false;
    }

    public String getValue(RequestIdentifier.Id identifier) {
        for (RequestIdentifier id : this.requestIds) {
            if (id.getIdName() == identifier) {
                return id.getIdValue();
            }
        }

        return null;
    }

    public RequestIdentifier get(RequestIdentifier.Id identifier) {
        for (RequestIdentifier id : this.requestIds) {
            if (id.getIdName() == identifier) {
                return id;
            }
        }

        return null;
    }
}
