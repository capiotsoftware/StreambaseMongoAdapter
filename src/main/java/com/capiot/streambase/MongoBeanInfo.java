package com.capiot.streambase;

import com.streambase.sb.operator.parameter.SBPropertyDescriptor;
import com.streambase.sb.operator.parameter.SBSimpleBeanInfo;

import java.beans.IntrospectionException;

/**
 * A BeanInfo class controls what properties are exposed, add
 * metadata about properties (such as which properties are optional), and access
 * special types of properties that can't be automatically derived via reflection.
 * If a BeanInfo class is present, only the properties explicitly declared in
 * this class will be exposed by StreamBase.
 */
public class MongoBeanInfo extends SBSimpleBeanInfo {

    /*
     * The order of properties below determines the order they are displayed within
     * the StreamBase Studio property view.
     */
    public SBPropertyDescriptor[] getPropertyDescriptorsChecked()
            throws IntrospectionException {
        SBPropertyDescriptor[] p = {
                new SBPropertyDescriptor("InsertSchema", Mongo.class)
                        .displayName("Insert Schema").description(""),
                new SBPropertyDescriptor("Url", Mongo.class).displayName(
                        "Mongo URL").description(""),
                new SBPropertyDescriptor("DB", Mongo.class).displayName(
                        "Mongo DB").description(""),
                new SBPropertyDescriptor("collection", Mongo.class)
                        .displayName("Collection Name").description(""),};
        return p;
    }

}
