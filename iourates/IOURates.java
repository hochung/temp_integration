package com.dataloom.integrations.iourates;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.integrations.EnhancedPropertyType;
import com.dataloom.integrations.IntegrationBase;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

/**
 * U.S. Electric Utility Companies and Rates: Look-up by Zipcode (Feb 2011)
 * Data: https://catalog.data.gov/dataset/u-s-electric-utility-companies-and-rates-look-up-by-zipcode-feb-2011-57a7c
 * @author Ho Chung Siu
 *
 */
public class IOURates {
    public static Set<EnhancedPropertyType> epts = new HashSet<>();
    public static EntityType et;
    public static EntitySet es;
        
    public static void prepare(){
        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "location", "us_zipcode" ),
                "US Zipcode",
                Optional.of( "US Zipcode" ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.String,
                true,
                row -> row.getAs( "zip" )
                ) );

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "eia", "idv2" ),
                "EIA IDv2",
                Optional.of( "US Energy Information Administration (EIA) Id" ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.Int32,
                false,
                row -> row.getAs( "eiaid" )
                ) );

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "electricutility", "name" ),
                "Utility Name",
                Optional.of( "The name of the utility company" ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.String,
                false,
                row -> row.getAs( "utility_name" )
                ) );

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "electricutility", "state" ),
                "State",
                Optional.of( "" ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.String,
                false,
                row -> row.getAs( "state" )
                ) );

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "electricutility", "service_type" ),
                "Service Type",
                Optional.of( "" ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.String,
                false,
                row -> row.getAs( "service_type" )
                ) );

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "electricutility", "ownership" ),
                "Ownership",
                Optional.of( "Ownership type" ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.String,
                false,
                row -> row.getAs( "ownership" )
                ) );

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "electricutility", "comm_rate" ),
                "Commerical rate",
                Optional.of( "The commercial electricity rate ($/kWh)" ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.Double,
                false,
                row -> row.getAs( "comm_rate" )
                ) );

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "electricutility", "ind_rate" ),
                "Industrial rate",
                Optional.of( "The industrial electricity rate ($/kWh)" ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.Double,
                false,
                row -> row.getAs( "ind_rate" )
                ) );

        epts.add( new EnhancedPropertyType(
                new FullQualifiedName( "electricutility", "res_rate" ),
                "Residential rate",
                Optional.of( "The residential electricity rate ($/kWh)" ), 
                ImmutableSet.of(), 
                EdmPrimitiveTypeKind.Double,
                false,
                row -> row.getAs( "res_rate" )
                ) );

        //UUID of key properties/properties can be random - the correct id will be used in integration; this is for bypassing constructor check
        et = new EntityType(
                new FullQualifiedName( "us", "electricutilityratesv2" ),
                "U.S. Electric Utility Companies and Rates (Trial 2)",
                "Average residential, commercial and industrial electricity rates by zip code for both investor owned utilities (IOU) and non-investor owned utilities.",
                ImmutableSet.of(),
                ImmutableSet.of(UUID.randomUUID()),
                ImmutableSet.of(UUID.randomUUID()) );
        
        es = new EntitySet(
                //This UUID can be random - the correct entity type id will be used in integration; this is for bypassing constructor check
                UUID.randomUUID(),
                "us_utility_rates_electric_feb2011v3",
                "U.S. Electric Utility Companies and Rates: Look-up by Zipcode (Feb 2011) (Trial 3)",
                Optional.of(
                        "This dataset, compiled by NREL using data from ABB, the Velocity Suite and the U.S. Energy Information Administration dataset 861, provides average residential, commercial and industrial electricity rates by zip code for both investor owned utilities (IOU) and non-investor owned utilities. Note: the file includes average rates for each utility, but not the detailed rate structure data found in the OpenEI U.S. Utility Rate Database. A more recent version of this data is also available through the NREL Utility Rate API with more search options. This data was released by NREL/Ventyx in February 2011." ) );
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
