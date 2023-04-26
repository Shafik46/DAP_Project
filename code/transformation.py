import psycopg2
# Connect to database
try:
    conn = psycopg2.connect(
        host="postgres",
        database="DAP_Project",
        user="airflow",
        password="airflow"
    )
    print("Connected to postgre server")
except psycopg2.Error as e:
    print(f"Error connecting to database: {e}")
    exit()

# Execute SQL command
cur = conn.cursor()
try:
    cur.execute('''
                -- Staging table type casting fields and handling null values
            insert into "DAP_Project"."Electric_Vehicles".ev_population_staging
            SELECT vin,census_tract, 
            base_msrp, city, 
            clean_alternative_fuel_vehicle_eligibility, 
            counties, county, 
            dol_vehicle_id, 
            electric_range, 
            COALESCE(electric_utility,'N/A') electric_utility, 
            electric_vehicle_type, 
            make,
            coalesce (model,'N/A') model,  
            model_year, 
            postal_code, 
            state
            FROM "DAP_Project"."Electric_Vehicles".ev_population
            ;
            commit;
            
            insert into "DAP_Project"."Electric_Vehicles".ev_population_fin
            SELECT distinct vin, census_tract, 
            base_msrp, city, 
            clean_alternative_fuel_vehicle_eligibility, 
            CASE 
                WHEN clean_alternative_fuel_vehicle_eligibility = 'Clean Alternative Fuel Vehicle Eligible' THEN 1
                WHEN clean_alternative_fuel_vehicle_eligibility = 'Not eligible due to low battery range' THEN 0
                ELSE 0
            END AS eligibility_status,
            counties, county, 
            dol_vehicle_id, 
            electric_range, 
            COALESCE(electric_utility,'N/A') electric_utility, 
            electric_vehicle_type, 
            make, 
            model, 
            model_year, 
            postal_code, 
            state
            FROM "DAP_Project"."Electric_Vehicles".ev_population_staging
            ;
            commit
            ''')
    conn.commit()
    print("SQL command executed successfully!")
except psycopg2.Error as e:
    print(f"Error executing SQL command: {e}")
    conn.rollback()

# Close database connection
cur.close()
conn.close()