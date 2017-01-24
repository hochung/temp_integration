package com.dataloom.integrations.chronicdiseases;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.integrations.EnhancedPropertyType;
import com.dataloom.integrations.IntegrationBase;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

/**
 * Schema: https://chronicdata.cdc.gov/api/views/g4ie-h725/
 * Dataset: https://catalog.data.gov/dataset/u-s-chronic-disease-indicators-cdi-e50c9/resource/61d71c08-f317-4af8-95b8-763f4bfadc4f
 * @author Ho Chung Siu
 *
 */
public class ChronicDiseases {
    public static Set<EnhancedPropertyType> epts = new HashSet<>();
    public static EntityType et;
    public static EntitySet es;
        
    public static void prepare(){
        //UUID of key properties/properties can be random - the correct id will be used in integration; this is for bypassing constructor check
        et = new EntityType(
                new FullQualifiedName( "diseases", "chronicdiseases" ),
                "U.S. Chronic Disease Indicators (CDI)",
                "",
                ImmutableSet.of(),
                ImmutableSet.of(UUID.randomUUID()),
                ImmutableSet.of(UUID.randomUUID()) );
        
        es = new EntitySet(
                //This UUID can be random - the correct entity type id will be used in integration; this is for bypassing constructor check
                UUID.randomUUID(),
                "chronicdiseases",
                "U.S. Chronic Disease Indicators (CDI)",
                Optional.of(
                        "CDC's Division of Population Health provides cross-cutting set of 124 indicators that were developed by consensus and that allows states and territories and large metropolitan areas to uniformly define, collect, and report chronic disease data that are important to public health practice and available for states, territories and large metropolitan areas. In addition to providing access to state-specific indicator data, the CDI web site serves as a gateway to additional information and data resources." ) );
    }
    

    public static void main( String args[] ) throws InterruptedException, JsonProcessingException, IOException{
        prepare();
        File file = new File( "schema.json" );
        IntegrationBase.integrate( "us_chronic_disease_indicators.csv", JsonSchemaReader.read( file ), et, es );
    }

}
