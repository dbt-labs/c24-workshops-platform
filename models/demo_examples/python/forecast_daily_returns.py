import pandas as pd # import packages
from prophet import Prophet

def model( dbt, session ):
    
    dbt.config(
        materialized="table", # the incremental materialization is also supported
        packages=['pandas==1.5.3','Prophet','holidays==0.18'] # how to import python libraries in dbt's context
    )

    # use historical data to fit model
    df = dbt.ref("agg_daily_returned_orders").to_pandas() # use dbt.ref to reference other models in your dbt project
    df.columns = df.columns.str.lower()
    m = Prophet()
    m.fit(df)
    
    # forecast returns and output dataframe
    future = m.make_future_dataframe(periods=365)
    df = m.predict(future)
    
    return df # return final dataset via data frame. This is required. 

# The Preview button in the IDE is disabled, but you can iterate by building the object and then opening a new tab and running... select * from ref('forecast_daily_returns')
# Tests and Documentation can also be applied to python models, in the same manner as sql models (.yml file or custom tests). 

