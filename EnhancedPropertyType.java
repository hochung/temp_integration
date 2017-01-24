package com.dataloom.integrations;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Row;

import com.dataloom.client.serialization.SerializableFunction;
import com.dataloom.edm.internal.PropertyType;
import com.google.common.base.Optional;

public class EnhancedPropertyType extends PropertyType {
    private boolean                           isKeyProperty;
    private SerializableFunction<Row, Object> mapper;

    public EnhancedPropertyType(
            UUID id,
            FullQualifiedName fqn,
            String title,
            Optional<String> description,
            Set<FullQualifiedName> schemas,
            EdmPrimitiveTypeKind datatype,
            boolean isKeyProperty,
            SerializableFunction<Row, Object> mapper ) {
        super( id, fqn, title, description, schemas, datatype );
        this.isKeyProperty = isKeyProperty;
        this.mapper = mapper;
    }

    public EnhancedPropertyType(
            FullQualifiedName fqn,
            String title,
            Optional<String> description,
            Set<FullQualifiedName> schemas,
            EdmPrimitiveTypeKind datatype,
            boolean isKey,
            SerializableFunction<Row, Object> mapper ) {
        super( fqn, title, description, schemas, datatype );
        this.isKeyProperty = isKey;
        this.mapper = mapper;
    }

    public boolean isKeyProperty() {
        return isKeyProperty;
    }

    public SerializableFunction<Row, Object> getMapper() {
        return mapper;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ( isKeyProperty ? 1231 : 1237 );
        result = prime * result + ( ( mapper == null ) ? 0 : mapper.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( !super.equals( obj ) ) return false;
        if ( getClass() != obj.getClass() ) return false;
        EnhancedPropertyType other = (EnhancedPropertyType) obj;
        if ( isKeyProperty != other.isKeyProperty ) return false;
        if ( mapper == null ) {
            if ( other.mapper != null ) return false;
        } else if ( !mapper.equals( other.mapper ) ) return false;
        return true;
    }

}
