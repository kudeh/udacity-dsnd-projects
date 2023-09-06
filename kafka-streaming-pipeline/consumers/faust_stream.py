"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

# initialize faust app
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

# define input topic
topic = app.topic("org.chicago.cta.stations", value_type=Station)

# define output Kafka Topic
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)

# Define a Faust Table
table = app.Table(
   "transformed_station",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)


# transform input `Station` records into `TransformedStation` records.
@app.agent(topic)
async def transform(stations):

    async for station in stations:
        
        if station.red:
            line = "red"
        elif station.blue:
            line = "blue"
        elif station.green:
            line = "green"
        else:
            logger.debug("Failed to extract color for station_id: %s", station.station_id)
            line = ''

        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line
        )
        table[station.station_id] = transformed_station


if __name__ == "__main__":
    app.main()
