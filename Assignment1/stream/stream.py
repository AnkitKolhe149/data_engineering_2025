import faust

app = faust.App('crypto-stream', broker='kafka://localhost:9092')
raw_topic = app.topic('api-raw-data', value_type=dict)
processed_topic = app.topic('api-processed-data', value_type=dict)

@app.agent(raw_topic)
async def process(records):
    async for record in records:
        try:
            if record.get("coin") in ["bitcoin", "ethereum"]:
                record["price_usd"] = record.get("price")
                await processed_topic.send(value=record)
                print("Processed:", record)
        except Exception as e:
            print("Stream processing error:", e)

if __name__ == '__main__':
    app.main()
