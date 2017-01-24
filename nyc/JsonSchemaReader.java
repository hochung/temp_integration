package com.dataloom.integrations.nyc;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.integrations.EnhancedPropertyType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

public class JsonSchemaReader {

    public static Set<EnhancedPropertyType> read( File file ) throws JsonProcessingException, IOException {
        Set<EnhancedPropertyType> epts = new HashSet<>();

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree( file );
        JsonNode items = root.get( "columns" );

        for ( JsonNode node : items ) {
            String name = node.get( "name" ).asText();
            String fieldName = node.get( "fieldName" ).asText();

            boolean isKey = false;
            if ( name.equals( "JURISDICTION NAME" ) ) {
                isKey = true;
            }

            EdmPrimitiveTypeKind dataType;
            if ( name.contains( "COUNT" ) ) {
                dataType = EdmPrimitiveTypeKind.Int32;
            } else if ( name.contains( "PERCENT" ) ) {
                dataType = EdmPrimitiveTypeKind.Double;
            } else {
                dataType = EdmPrimitiveTypeKind.String;
            }

            epts.add( new EnhancedPropertyType(
                    new FullQualifiedName( "nyc", fieldName ),
                    name,
                    Optional.of( "" ),
                    ImmutableSet.of(),
                    dataType,
                    isKey,
                    row -> row.getAs( name ) ) );
        }

        return epts;
    }
}
