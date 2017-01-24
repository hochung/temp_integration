package com.dataloom.integrations.nyc;

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
import com.dataloom.integrations.slc.Slc2;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

/**
 * Demographic statistics broken down by zip code
 * Dataset from https://catalog.data.gov/dataset/demographic-statistics-by-zip-code-acfc9/resource/e43f1938-3c4a-4501-9aaf-46891bb21553
 * @author Ho Chung Siu
 *
 */
public class NycDemographics {
    public static Set<EnhancedPropertyType> epts = new HashSet<>();
    public static EntityType et;
    public static EntitySet es;
        
    public static void prepare(){
        //UUID of key properties/properties can be random - the correct id will be used in integration; this is for bypassing constructor check
        et = new EntityType(
                new FullQualifiedName( "nyc", "demographicsstats" ),
                "Demographic Statistics By Zip Code",
                "Demographic statistics broken down by zip code",
                ImmutableSet.of(),
                ImmutableSet.of(UUID.randomUUID()),
                ImmutableSet.of(UUID.randomUUID()) );
        
        es = new EntitySet(
                //This UUID can be random - the correct entity type id will be used in integration; this is for bypassing constructor check
                UUID.randomUUID(),
                "nycdemostats",
                "Demographic Statistics By Zip Code",
                Optional.of(
                        "Demographic Statistics By Zip Code by the Department of Youth and Community Development (DYCD)" ) );
    }
    

    public static void main( String args[] ) throws InterruptedException, JsonProcessingException, IOException{
        prepare();
        File file = new File( "nyc_schema.json" );
        IntegrationBase.integrate( "nyc_demo_stats.csv", JsonSchemaReader.read( file ), et, es );
    }
}
