package com.dataloom.integrations.la;

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

import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.integrations.EnhancedPropertyType;
import com.dataloom.integrations.IntegrationBase;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

/**
 * DataSet: https://catalog.data.gov/dataset/crimes-2012-2015/resource/a2369c45-fcdb-4819-b3fc-979c7b737aed
 * @author Ho Chung Siu
 *
 */
public class Crimes2012to2015 {
    public static Set<EnhancedPropertyType> epts = new HashSet<>();
    public static EntityType et;
    public static EntitySet es;
    
    private static Pattern            p        = Pattern.compile( ".*\\((.+),.([-].+)\\)" );
        
    public static void prepare(){
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "lapd", "datereported" ),
                "Date Reported",
                Optional.of( "Date Reported" ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.Date,
                false,
                row -> row.getAs( "Date.Rptd" )
                ) );
        
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "lapd", "dr_no" ),
                "Dr No.",
                Optional.of( "Dr No." ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.Int64,
                true,
                row -> row.getAs( "DR.NO" )
                ) );
        
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "lapd", "date_occ" ),
                "Date occured",
                Optional.of( "Occurence date of the crime." ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.Date,
                false,
                row -> row.getAs( "DATE.OCC" )
                ) );
        
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "lapd", "time_occ" ),
                "Time occured",
                Optional.of( "Occurence time of the crime." ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.Int32,
                false,
                row -> row.getAs( "TIME.OCC" )
                ) );
        
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "lapd", "area" ),
                "Area",
                Optional.of( "Area number of crime location." ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.Int32,
                false,
                row -> row.getAs( "AREA" )
                ) );
        
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "lapd", "area_name" ),
                "Area Name",
                Optional.of( "Area name of crime location." ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.String,
                false,
                row -> row.getAs( "AREA.NAME" )
                ) );
        
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "lapd", "rd" ),
                "RD",
                Optional.of( "Not sure what this means." ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.Int32,
                false,
                row -> row.getAs( "RD" )
                ) );
        
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "lapd", "crm_cd" ),
                "Crm.Cd",
                Optional.of( "Not sure what this means." ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.Int32,
                false,
                row -> row.getAs( "Crm.Cd" )
                ) );
        
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "lapd", "crmcd_desc" ),
                "CrmCd Description",
                Optional.of( "Not sure what this means." ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.String,
                false,
                row -> row.getAs( "CrmCd.Desc" )
                ) );
        
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "lapd", "status" ),
                "Status",
                Optional.of( "Case status (Abbreviation)" ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.String,
                false,
                row -> row.getAs( "Status" )
                ) );
        
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "lapd", "status_desc" ),
                "Status Description",
                Optional.of( "Case status (Description)" ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.String,
                false,
                row -> row.getAs( "Status.Desc" )
                ) );
        
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "lapd", "location" ),
                "Location",
                Optional.of( "Location of the crime" ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.String,
                false,
                row -> row.getAs( "LOCATION" )
                ) );
        
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "lapd", "cross_street" ),
                "Cross Street",
                Optional.of( "Cross street of the crime" ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.String,
                false,
                row -> row.getAs( "Cross.Street" )
                ) );
        
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "location", "latitude" ),
                "Latitude",
                Optional.of( "" ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.Double,
                false,
                Crimes2012to2015::getLat
                ) );

        
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "location", "longitude" ),
                "Longitude",
                Optional.of( "" ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.Double,
                false,
                Crimes2012to2015::getLon
                ) );

        
        //UUID of key properties/properties can be random - the correct id will be used in integration; this is for bypassing constructor check
        et = new EntityType(
                new FullQualifiedName( "lapd", "crimes" ),
                "Crime statistics in City of LA",
                "Raw crime data of City of LA",
                ImmutableSet.of(),
                ImmutableSet.of(UUID.randomUUID()),
                ImmutableSet.of(UUID.randomUUID()) );
        
        es = new EntitySet(
                //This UUID can be random - the correct entity type id will be used in integration; this is for bypassing constructor check
                UUID.randomUUID(),
                "lacrimes2012to2015",
                "Crime statistics in City of LA from 2012 to 2015",
                Optional.of(
                        "This is the combined raw crime data for 2012 through 2015. Currently, crime data is refreshed on an annual basis." ) );
    }
    
    
    public static double getLat( Row row ) {
        String location = row.getAs( "Location.1" );
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
        String location = row.getAs( "Location.1" );
        if ( StringUtils.isBlank( location ) ) {
            return 0D;
        }
        Matcher m = p.matcher( location );
        if ( !m.matches() ) {
            return 0D;
        }
        return Double.parseDouble( m.group( 2 ) );
    }

    public static void main( String args[] ) throws InterruptedException, JsonProcessingException, IOException{
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
