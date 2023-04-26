-- Staging table type casting fields and handling null values
insert into "DAP_Project"."Electric_Vehicles".ev_population_staging
select vin,
census_tract, 
base_msrp, city, 
clean_alternative_fuel_vehicle_eligibility, 
counties, county, 
dol_vehicle_id, 
electric_range, 
COALESCE(electric_utility,'N/A') electric_utility, 
electric_vehicle_type, 
make, 
model, 
model_year, 
postal_code, 
state,
now() Batch_time
FROM "DAP_Project"."Electric_Vehicles".ev_population
;
commit;

insert into "DAP_Project"."Electric_Vehicles".ev_population_final
SELECT distinct vin,
census_tract, 
base_msrp, city, 
clean_alternative_fuel_vehicle_eligibility, 
counties, county, 
dol_vehicle_id, 
electric_range, 
COALESCE(electric_utility,'N/A') electric_utility, 
electric_vehicle_type, 
make, 
model, 
model_year, 
postal_code, 
state,
Batch_time
FROM "DAP_Project"."Electric_Vehicles".ev_population_staging
;
commit
