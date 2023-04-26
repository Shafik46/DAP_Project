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
            insert into "DAP_Project"."Electric_Vehicles".ev_registration_fin 
            SELECT distinct
            vin, 
            clean_alternative_fuel_vehicle_type, 
            dol_vehicle_id, 
            model_year, 
            make,
            coalesce (model,'N/A') model, 
            vehicle_primary_use, 
            electric_range, 
            odometer_reading, 
            odometer_code, 
            new_or_used_vehicle, 
            sale_price, 
            sale_date, 
            base_msrp, 
            transaction_type, 
            dol_transaction_date, 
            transaction_year, 
            county, 
            city, 
            state_of_residence, 
            postal_code,  
            replace(hb_2778_exemption_eligibility, 'HB 2778 Eligiblity Requirements', '') as hb_2778_exemption_eligibility,
            replace(hb_2042_clean_alternative_fuel_vehicle_eligibility, 'HB 2042 Eligibility Requirements', '') as hb_2042_clean_alternative_fuel_vehicle_eligibility, 
            meets_2019_hb_2042_electric_range_requirement, 
            meets_2019_hb_2042_sale_date_requirement, 
            meets_2019_hb_2042_sale_price_value_requirement, 
            hb_2042_battery_range_requirement, 
            hb_2042_purchase_date_requirement, 
            hb_2042_sale_price_value_requirement, 
            electric_vehicle_fee_paid, 
            transportation_electrification_fee_paid, 
            coalesce (hybrid_vehicle_electrification_fee_paid,'Not Applicable') hybrid_vehicle_electrification_fee_paid,  
            coalesce (electric_utility,'N/A') electric_utility 
            FROM "DAP_Project"."Electric_Vehicles".ev_registration
            ;

            ''')
    conn.commit()
    print("SQL command executed successfully!")
except psycopg2.Error as e:
    print(f"Error executing SQL command: {e}")
    conn.rollback()

# Close database connection
cur.close()
conn.close()