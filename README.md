# University project for a subject "Architectures of Big Data Systems"

Author: Mihajlo Perendija


# Goal

Create a system for real-time notifying about reported crimes in the city of Chicago and analysis of historical data of reported crimes combined with other relevant data.

# Data

Data used in this project consists of:

- [Crimes - 2001 to Present](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2) - Data of reported crimes in Chicago from 2001 to Present in form of a csv file. Attributes of a single report can be seen at the given link. Not available in the repository because of its size (~1.7GB).
- [Police Stations](https://data.cityofchicago.org/Public-Safety/Police-Stations/z8bn-74gv) - Consists of data about police stations - located at *consumers/map/Police_Stations.csv*
- [Boundaries - Police Districts](https://data.cityofchicago.org/Public-Safety/Boundaries-Police-Districts-current-/fthy-xz3r) - Exported as geojson, represents police district boundaries - located at *consumers/map/Boundaries - Police Districts (current).geojson*
- Census Data By Community Area - Found in pdf form, transformed to csv, consists of data about population in all community areas in Chicago - located at *data/Census_Data_By_Community_Area.csv*
- Not used in project but available in repo: Police Beats, Census Data - Socioeconomics data

Preparation of data:
After downloading "Crimes - 2001 to Present" dataset it should be split in two files: 
- data_batch.csv - Consisting of the majority of the data from the dataset as it is used in batch processing of historical data. This file should be placed in *data* folder.
- data_realtime.csv - Consisting of the last, small part of the dataset as it will be used to generate "new" data for real-time processing. This file should be placed in *producer* folder. Also, it should be cleaned of Null values for locations.

# Architecture

![Architecture](https://github.com/mihajlo-perendija/ASVSP/blob/master/system_architecture.png)
