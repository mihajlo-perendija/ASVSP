import faust
from datetime import timedelta
import time
import json

app = faust.App(id="test",broker="kafka://localhost:9092",store="memory://",topic_partitions=1)

# convenience func for launching the app
def main() -> None:
    app.main()


input_topic = app.topic('crime-reporting', internal=False, partitions=1)
realtime_topic = app.topic('realtime-topic', internal=True, partitions=1, retention=timedelta(seconds=120),  value_serializer='json', deleting=True)


district_crimes_count = app.Table(
    'district_crimes_count',
    default=int,
    key_type=str,
    value_type=int,
    partitions=1,
).tumbling(
    timedelta(minutes=2),
    expires=timedelta(minutes=4)
)

@app.agent(input_topic)
async def process(stream):
    await realtime_topic.declare()
    async for value in stream:
        print(value)
        await realtime_topic.send(value=value)


if __name__ == '__main__':
    app.main()
