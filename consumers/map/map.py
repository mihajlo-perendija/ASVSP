import json
import csv
import time

from bokeh.io import show
from bokeh.models import (CDSView, ColorBar, ColumnDataSource,
                          CustomJS, CustomJSFilter, 
                          GeoJSONDataSource, HoverTool,
                          LinearColorMapper, Slider)
from bokeh.layouts import column, row, widgetbox
from bokeh.palettes import brewer
from bokeh.plotting import figure, curdoc

from threading import Thread

import geopandas as gpd

from kafka import KafkaConsumer


############################
## Map setup
police_stations = csv.reader(open('Police_Stations.csv'), delimiter=',')
headers = next(police_stations, None)
police_stations_list = list(police_stations)
police_stations_list_dict=dict(Latitude=[],
              Longitude=[])
for station in police_stations_list:
    police_stations_list_dict['Latitude'].append(float(station[12]))
    police_stations_list_dict['Longitude'].append(float(station[13]))


districts = gpd.read_file('Boundaries - Police Districts (current).geojson')
districts['crimes'] = 0
# Input GeoJSON source that contains features for plotting
geosource = GeoJSONDataSource(geojson = districts.to_json())

# Define color palettes
palette = brewer['Reds'][9]
palette = palette[::-1] # reverse order of colors so higher values have darker colors
# Instantiate LinearColorMapper that linearly maps numbers in a range, into a sequence of colors.
color_mapper = LinearColorMapper(palette = palette, low = 0, high = 50)
# Define custom tick labels for color bar.
tick_labels = {'0': '0', '5000000': '5,000,000',
 '10000000':'10,000,000', '15000000':'15,000,000',
 '20000000':'20,000,000', '25000000':'25,000,000',
 '30000000':'30,000,000', '35000000':'35,000,000',
 '40000000':'40,000,000+'}
# Create color bar.
color_bar = ColorBar(color_mapper = color_mapper, 
                     label_standoff = 8,
                     width = 500, height = 20,
                     border_line_color = None,
                     location = (0,0), 
                     orientation = 'horizontal',
                     major_label_overrides = tick_labels)
# Create figure object.
p = figure(title = 'Crimes in Chicago Live Map', 
           toolbar_location = 'below',
           tools = 'pan, wheel_zoom, box_zoom, reset')
p.xgrid.grid_line_color = None
p.ygrid.grid_line_color = None

police_stations_source = ColumnDataSource(data=police_stations_list_dict)
police_stations_circles = p.circle(x="Longitude", y="Latitude", size=5, fill_color="blue", fill_alpha=1, source=police_stations_source)

# Add patch renderer to figure.
districts_patches = p.patches('xs','ys', source = geosource,
                   fill_color = {'field' :'crimes',
                                 'transform' : color_mapper},
                   line_color = 'gray', 
                   line_width = 0.25, 
                   fill_alpha = 0.6)
# Create hover tool
p.add_tools(HoverTool(renderers = [districts_patches],
                      tooltips = [('District','@dist_label'),
                               ('Crimes', '@crimes')]))

crimes_dict = dict(Latitude=[],
              Longitude=[],
              Date=[],
              PrimaryType=[],
              Description=[])
crimes_source = ColumnDataSource(data=crimes_dict)

crime_circles = p.circle(x="Longitude", y="Latitude", size=5, fill_color="red", line_color = None, fill_alpha=0.8, source=crimes_source)
p.add_tools(HoverTool(renderers = [crime_circles],
                      tooltips = [('Primary Type','@PrimaryType'),
                               ('Description', '@Description'),
                               ('Date', '@Date')]))
############################


############################
## Map updating logic
pending_crimes =  []

def update_crimes(crimes):
    global crimes_dict
    for crime in crimes:
        crimes_dict['Latitude'].append(float(crime[19]))
        crimes_dict['Longitude'].append(float(crime[20]))
        crimes_dict['Date'].append(crime[2])
        crimes_dict['PrimaryType'].append(crime[5])
        crimes_dict['Description'].append(crime[6])
    crime_circles.data_source.data = crimes_dict

def update_districts(crimes):
    global districts
    for crime in crimes:
        dist = crime[11].split('.')[0]
        ind = districts.index[districts['dist_num'] == dist].tolist()[0]
        districts.at[ind, 'crimes'] = int(districts.at[ind, 'crimes']) + 1
    districts_patches.data_source.geojson = districts.to_json()

def update_map():
    global pending_crimes
    crimes = pending_crimes[:]
    pending_crimes = []
    update_districts(crimes)
    update_crimes(crimes)

############################

doc = curdoc().add_root(column(p))
curdoc().add_periodic_callback(update_map, 2000)


def run_consumer():
    consumer = KafkaConsumer(
        'realtime-topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')))

    for message in consumer:
        value = message.value
        pending_crimes.append(value)
        print(message)

thread = Thread(target=run_consumer)
thread.daemon = True
thread.start()

# def run_consumer_batch():
#     global pending_crimes
#     consumer = KafkaConsumer(
#         'realtime-topic',
#         bootstrap_servers=['localhost:9092'],
#         auto_offset_reset='earliest',
#         enable_auto_commit=True,
#         group_id=None,
#         value_deserializer=lambda v: json.loads(v.decode('utf-8')))
#     while True:
#         msg_pack = consumer.poll(timeout_ms=6000, max_records=5000, update_offsets=False)
#         pending_crimes = []
#         for tp, messages in msg_pack.items():
#             for message in messages:
#                 value = message.value
#                 print(value)
#                 pending_crimes.append(value)
#         time.sleep(15)
#         consumer.seek_to_beginning()

# thread2 = Thread(target=run_consumer_batch)
# thread2.daemon = True
# thread2.start()