package com.dataloom.integrations.slc;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Row;

import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.integrations.EnhancedPropertyType;
import com.dataloom.integrations.IntegrationBase;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

public class Slc2 {
    public static Set<EnhancedPropertyType> epts   = new HashSet<>();
    public static EntityType                et;
    public static EntitySet                 es;

    private static Pattern                  p      = Pattern.compile( ".*\\n*.*\\n*\\((.+),(.+)\\)" );
    private static final Random             random = new Random();

    public static void prepare() {
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "publicsafety", "case" ),
                "Case #",
                Optional.of( "The case it was filed under" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String,
                true,
                row -> row.getAs( "CASE" ) ) );

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "publicsafety", "offensecode" ),
                "Offense Code",
                Optional.of( "The code of the offense" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String,
                false,
                row -> row.getAs( "OFFENSE CODE" ) ) );

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "publicsafety", "offensedescription" ),
                "Offense Description",
                Optional.of( "The description of the offense" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String,
                false,
                row -> row.getAs( "OFFENSE DESCRIPTION" ) ) );

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "publicsafety", "reportdate" ),
                "Report Date",
                Optional.of( "The day the car was reported stolen" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String,
                false,
                row -> row.getAs( "REPORT DATE" ) ) );

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "publicsafety", "occdate" ),
                "Offense Date",
                Optional.of( "The day the car was stolen" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String,
                false,
                row -> row.getAs( "OCC DATE" ) ) );

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "general", "dayofweek" ),
                "Day Of Week",
                Optional.of( "Day of week car was stolen" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.Int16,
                false,
                row -> {
                    try {
                        System.out.println( "Trying to read INT: " + row.getAs( "DAY OF WEEK" ) );
                        return Integer.parseInt( row.getAs( "DAY OF WEEK" ) );
                    } catch ( NumberFormatException e ) {
                        System.err.println( "Failed to read INT: " + row.getAs( "DAY OF WEEK" ) );
                        return null;
                    }
                } ) );

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "general", "address" ),
                "Address",
                Optional.of( "Address the car was stolen from" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String,
                false,
                row -> row.getAs( "LOCATION" ) ) );

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "location", "latitude" ),
                "Latitude",
                Optional.of( "" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.Double,
                false,
                Slc2::getLat ) );

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "location", "longitude" ),
                "Longitude",
                Optional.of( "" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.Double,
                false,
                Slc2::getLon ) );

        // UUID of key properties/properties can be random - the correct id will be used in integration; this is for
        // bypassing constructor check
        et = new EntityType(
                new FullQualifiedName( "publicsafety", "stolencars" ),
                "Stolen cars in Salt Lake City",
                "Stolen cars in Salt Lake City",
                ImmutableSet.of(),
                ImmutableSet.of( UUID.randomUUID() ),
                ImmutableSet.of( UUID.randomUUID() ) );

        es = new EntitySet(
                // This UUID can be random - the correct entity type id will be used in integration; this is for
                // bypassing constructor check
                UUID.randomUUID(),
                "slcstolencars2012",
                "Salt Lake Lake City Stolen Cars (2012)",
                Optional.of(
                        "All cars stolen in Salt Lake City in 2012." ) );
    }

    public static double getLat( Row row ) {
        String location = row.getAs( "LOCATION" );
        if ( StringUtils.isBlank( location ) ) {
            return 0D;
        }
        Matcher m = p.matcher( location );
        if ( !m.matches() ) {
            return 0D;
        }
        return Double.parseDouble( m.group( 1 ) );
    }

    public static double getLon( Row row ) {
        String location = row.getAs( "LOCATION" );
        if ( StringUtils.isBlank( location ) ) {
            return 0D;
        }
        Matcher m = p.matcher( location );
        if ( !m.matches() ) {
            return 0D;
        }
        return Double.parseDouble( m.group( 2 ) );
    }

    public static void main( String args[] ) throws InterruptedException, JsonProcessingException, IOException {
        int length = args.length;
        if ( length < 2 ) {
            throw new IllegalArgumentException( "Required arguments: JWT Token, Csv Location" );
        }
        String jwtToken = args[ 0 ];
        String csvLocation = args[ 1 ];

        prepare();

        IntegrationBase.integrate( jwtToken, csvLocation, epts, et, es );
    }
}
