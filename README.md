# Real time workshop

## Overview

This workshop is designed to provide hands-on experience with real-time data processing using Snowflake and Streamlit. The use case we will work with extracts real-time Bicing data, loads it to an S3 bucket, then Snowflake consumes it and transforms this data to serve a Streamlit. The final product is a real-time dashboard that indicates which stations need repair and what path should the Bicing truck follow in order to have availability of free docks and bikes.

## Crash course on streaming, events and real time

[ ] explain basic concepts
[ ] provide an overview diagram of all the infrastructure

## Steps to Follow

Now that we have a grasp of some theroy, let's start building our application to process and activate Bicing data.

### Snowflake

1. **Create a Snowflake Free Account**: Before getting started, you need to create a Snowflake free account. Visit the [Snowflake website](https://www.snowflake.com/) and sign up for a free account if you don't have one already. Once you have your account provisioned, we can start to run the scripts in the next section.

2. **Run the setup and pipeline scripts**: all the SQL code necessary to build our app can be found in `/sql_scripts`.

    - `00_setup.sql`: create the database structure and change to SYSADMIN role.
    - `01_ingest_pipeline.sql`: create and configure various objects to build the ingestion pipeline that will continuously load data from S3 to Snowflake. Also ingest the static master data to the final msater table.
    - `02_transform.sql`: finally, create the final step of the pipeline where we transform the data we have loaded to serve the streamlit application.

3. **Create the views that will feed the application**: in order to simplify the application code (it's a simple streamlit front) we will create a couple of views that will store the logic to obtain our data modeled in the way the application will use it.

    - `11_V_BROKEN_DOCKERS.sql`: This view compares the total number of docks from the master data to the total number of bikes and free docks available at each station. This will be used to show which stations need dock repair.
    - `12_V_BIKE_DISTRIBUTION`: This view
    [ ] finish

**Bonus**: here we have a detailed diagram that pictures all the objects we just created and their relations.

[ ] draw the diagram

### Streamlit

1. **Create a Streamlit Account**: To visualize and interact with the real-time data, you will create a Streamlit account and deploy a Streamlit app. First visit the [Streamlit website](https://www.streamlit.io/) and sign up for an account.

2. **Deploy the app**:

[ ] finish explanation

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.
