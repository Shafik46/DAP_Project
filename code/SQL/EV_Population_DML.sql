CREATE TABLE "DAP_Project"."Electric_Vehicles".EV_Population (
  Census_Tract TEXT,
  Base_MSRP FLOAT,
  City TEXT,
  Clean_Alternative_Fuel_Vehicle_Eligibility TEXT,
  Congressional_Districts TEXT,
  Counties TEXT,
  County TEXT,
  DOL_Vehicle_ID TEXT,
  Electric_Range FLOAT,
  Electric_Utility TEXT,
  Electric_Vehicle_Type TEXT,
  Legislative_District TEXT,
  Make TEXT,
  Model TEXT,
  Model_Year INT,
  Postal_Code TEXT,
  State TEXT,
  VIN TEXT,
  Vehicle_Location TEXT,
  WAOFM_GIS_Legislative_District_Boundary TEXT
);

create table "DAP_Project"."Electric_Vehicles".ev_population_staging as
            SELECT vin,census_tract,
            base_msrp, city,
            clean_alternative_fuel_vehicle_eligibility,
            counties, county,
            dol_vehicle_id,
            electric_range,
            COALESCE(electric_utility,'N/A') electric_utility,
            electric_vehicle_type,
            coalesce (model,'N/A') model,
            make,
            model_year,
            postal_code,
            state
            FROM "DAP_Project"."Electric_Vehicles".ev_population
            where 1=2
            ;

            CREATE TABLE "DAP_Project"."Electric_Vehicles".ev_population_fin AS
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
            where 1=2
            ;
