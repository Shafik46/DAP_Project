import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns

sql = '''select * from "DAP_Project"."Electric_Vehicles".ev_population_fin'''
try:
    dbConnection = psycopg2.connect(
        user = "airflow",
        password = "airflow",
        host = "localhost",
        port = "5432",
        database = "DAP_Project"
    )
    ev_pop_df = sqlio.read_sql_query(sql, dbConnection)
except (Exception , psycopg2.Error) as dbError :
    print ("Error:", dbError)
finally:
    if dbConnection in locals():
        dbConnection.close()

# Compute the correlation matrix
corr_matrix = ev_pop_df.corr()

# Visualize the correlation matrix as a heatmap using seaborn
sns.heatmap(corr_matrix, annot=True)

# Save the plot as an image
plt.savefig('pop_corr.png')