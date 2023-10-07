# Toronto Shelter Data Project - Data Analysis for Homeless Shelter Occupancy Modeling

## Project Overview

This repository contains [DBT](https://docs.getdbt.com/) project for modeling overnight service occupancy in homeless shelters in the city of Toronto. Utilizing DBT (Data Build Tool) and Google BigQuery, we transform raw data, including shelter data and weather data, to create specific tables optimized for subsequent modeling processes.

### Objectives

- **Data Transformation**: Utilize DBT to transform raw shelter and weather data.
- **Data Modeling**: Prepare data models to analyze factors influencing homeless shelter occupancy.
- **Insight Generation**: Generate insights that could inform decision-making related to shelter management and resource allocation.

## Data Sources

- **Shelter Data**: [Toronto Open Data](https://open.toronto.ca/dataset/daily-shelter-overnight-service-occupancy-capacity/)
- **Weather Data**: [NOAA Big Query Data](https://console.cloud.google.com/marketplace/product/noaa-public/gsod?project=toronto-shelter-project) for historical weather data and [AccuWeather API](https://developer.accuweather.com/) for weather forecast data.

## Technology Stack

- **[DBT](https://www.getdbt.com/)**: For data transformation and modeling.
- **[Google BigQuery](https://cloud.google.com/bigquery)**: As the data warehouse for storing and querying the dataset.
- **[Posit](https://posit.co/)**: For machine learning and reporting.

## Repository Structure

- `/analysis`: Contains dbt models, tests, and transformation scripts.
- `/macros`: Additional documentation or related resources.
- `/models`: Data models.
- `/seeds`: Jupyter notebooks or R Markdown files for exploratory data analysis.
- `/snapshots`: Sample data or mock data for testing or illustrative purposes.
- `/tests`: For tests.
- `dbt_project.yml`: Project `yml` file.

## Resources

Some additional resources that helped with setting up DBT Cloud;

- DBT Cloud & Big Query Setup - [Youtube](https://www.youtube.com/watch?v=COeMn18qSkY&list=PL0QYlrC86xQlp-eOGzGllDxYese4Ki_6A&index=3)
- Big Query Authentication - [Stackoverflow](https://stackoverflow.com/questions/42410147/how-to-authenticate-with-service-account-and-bigrquery-package)
- Weather Data in Big Query - [Stackoverflow](https://stackoverflow.com/questions/34804654/how-to-get-the-historical-weather-for-any-city-with-bigquery)
