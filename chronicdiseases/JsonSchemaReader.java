package com.dataloom.integrations.chronicdiseases;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Row;

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
            String dataTypeName = node.get( "dataTypeName" ).asText();

            boolean isKey = false;
            if( name.equals( "YearStart" ) || name.equals( "LocationAbbr" ) || name.equals( "DataValueType" ) || name.equals( "Stratification1" ) ){
                isKey = true;
            }
            
            EdmPrimitiveTypeKind dataType;
            if ( name.equals( "DataValue" ) || name.equals( "DataValueAlt" ) || name.equals( "LowConfidenceLimit" )
                    || name.equals( "HighConfidenceLimit" ) ) {
                dataType = EdmPrimitiveTypeKind.Double;
            } else if ( dataTypeName.equals( "number" ) ) {
                dataType = EdmPrimitiveTypeKind.Int32;
            } else if ( dataTypeName.equals( "location" ) ) {
                dataType = EdmPrimitiveTypeKind.Double;
            } else {
                dataType = EdmPrimitiveTypeKind.String;
            }

            if ( !dataTypeName.equals( "location" ) ) {
                epts.add( new EnhancedPropertyType(
                        new FullQualifiedName( "diseases", fieldName ),
                        name,
                        Optional.of( "" ),
                        ImmutableSet.of(),
                        dataType,
                        isKey,
                        row -> row.getAs( name ) ) );
            } else {
                epts.add( new EnhancedPropertyType(
                        new FullQualifiedName( "location", "latitude" ),
                        name + "_latitude",
                        Optional.of( "" ),
                        ImmutableSet.of(),
                        dataType,
                        isKey,
                        row -> getLat( name, row ) ) );

                epts.add( new EnhancedPropertyType(
                        new FullQualifiedName( "location", "longitude" ),
                        name + "_longitude",
                        Optional.of( "" ),
                        ImmutableSet.of(),
                        dataType,
                        isKey,
                        row -> getLon( name, row ) ) );
            }
        }

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "diseases", "id" ),
                "Entry Id",
                Optional.of( "" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.Guid,
                true,
                row -> UUID.randomUUID() ) );
        return epts;
    }

    private static Pattern p = Pattern.compile( ".*\\((.+),(.+)\\).*" );

    public static double getLat( String name, Row row ) {
        String location = row.getAs( name );
        if ( StringUtils.isBlank( location ) ) {
            return 0D;
        }
        Matcher m = p.matcher( location );
        if ( !m.matches() ) {
            return 0D;
        }
        return Double.parseDouble( m.group( 1 ) );
    }

    public static double getLon( String name, Row row ) {
        String location = row.getAs( name );
        if ( StringUtils.isBlank( location ) ) {
            return 0D;
        }
        Matcher m = p.matcher( location );
        if ( !m.matches() ) {
            return 0D;
        }
        return Double.parseDouble( m.group( 2 ) );
    }
}
