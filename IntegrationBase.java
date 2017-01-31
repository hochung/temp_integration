package com.dataloom.integrations;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.auth0.Auth0;
import com.auth0.authentication.AuthenticationAPIClient;
import com.auth0.request.AuthenticationRequest;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.exceptions.TypeExistsException;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.integrations.slc.SlcStolenCars;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.shuttle.EntityDefinition;
import com.kryptnostic.shuttle.Flight;
import com.kryptnostic.shuttle.Shuttle;

import retrofit2.Retrofit;

public class IntegrationBase {
    private static final SparkSession sparkSession;
    private static final Logger logger = LoggerFactory.getLogger( SlcStolenCars.class );

    private static final Auth0                             auth0               = new Auth0(
            "PTmyExdBckHAiyOjh4w2MqSIUGWWEdf8",
            "loom.auth0.com" );
    private static final AuthenticationAPIClient           client              = auth0.newAuthenticationAPIClient();
    private static Environment environment;
    private static EdmApi edm;

    private static Set<UUID> ALL_PROPERTY_IDS = new HashSet<>();
    private static Set<UUID> KEY_PROPERTY_IDS = new HashSet<>();
    private static List<FullQualifiedName> KEY_PROPERTY_FQNS = new ArrayList<>();
    
    /**
     * Configuration of integration
     */
    static {
        sparkSession = SparkSession.builder()
                .master( "local[5]" )
                .appName( "test" )
                .getOrCreate();
        
        /**        
        AuthenticationRequest request = client.login( "support@kryptnostic.com", "abracadabra" )
                .setConnection( "Tests" )
                .setScope( "openid email nickname roles user_id" );
        jwtToken = request.execute().getIdToken();
        logger.info( "Using the following idToken: Bearer {}" , jwtToken );
        environment = Environment.LOCAL;
        */

//        environment = Environment.PRODUCTION;
        environment = Environment.LOCAL;
    }

    public static void integrate( String jwtToken, String filePath, Set<EnhancedPropertyType> epts, EntityType et, EntitySet es ) throws InterruptedException {
        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        edm = retrofit.create( EdmApi.class );

        createPropertyTypes( epts );
        UUID etId = createEntityType( et );
        createEntitySet( es, etId );
        
//        String path = new File( IntegrationBase.class.getClassLoader().getResource( fileName ).getPath() ).getAbsolutePath();
        String path = new File( filePath ).getAbsolutePath();
        runIntegration( jwtToken, path, epts, et, es );
    }
    
    private static void createPropertyTypes( Set<EnhancedPropertyType> epts ){
        
        for( EnhancedPropertyType ept : epts ){
            UUID id = edm.createPropertyType( new PropertyType(
                    ept.getType(),
                    ept.getTitle(),
                    Optional.of( ept.getDescription() ),
                    ept.getSchemas(),
                    ept.getDatatype()
                    ) );
            if( id == null) {
                // property type created
                id = edm.getPropertyTypeId( ept.getType().getNamespace(), ept.getType().getName() );
            }
            ALL_PROPERTY_IDS.add( id );
            if( ept.isKeyProperty() ){
                KEY_PROPERTY_IDS.add( id );
                KEY_PROPERTY_FQNS.add( ept.getType() );   
            }
        }
    }
    
    private static UUID createEntityType( EntityType et ){
        UUID id = edm.createEntityType( new EntityType(
                et.getType(),
                et.getTitle(),
                et.getDescription(),
                et.getSchemas(),
                KEY_PROPERTY_IDS,
                ALL_PROPERTY_IDS ) );
        if( id == null){
            id = edm.getEntityTypeId( et.getType().getNamespace(), et.getType().getName() );
        }
        return id;
    }
    
    private static void createEntitySet( EntitySet es, UUID entityTypeId){
        Map<String, UUID> map = edm.createEntitySets( ImmutableSet.of( new EntitySet(
                entityTypeId,
                es.getName(),
                es.getTitle(),
                Optional.of( es.getDescription() )
                ) ) );
        if( map == null ){
            logger.error( "Entity Set already exists" );
            // throw new IllegalArgumentExeption( "Entity Set already exists" );
        }
    }

    private static void runIntegration( String jwtToken, String path, Set<EnhancedPropertyType> epts, EntityType et, EntitySet es ) throws InterruptedException{
        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( path );

        EntityDefinition.Builder builder = Flight.newFlight()
                .addEntity().to( es.getName() ).as( et.getType() )
                .key( KEY_PROPERTY_FQNS.toArray( new FullQualifiedName[]{} ) );
        
        for( EnhancedPropertyType ept : epts ){
            builder.addProperty().value( ept.getMapper() ).as( ept.getType() ).ok();
        }
        
        Flight flight = builder.ok().done();

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        shuttle.launch( flight, payload );
    }
}
