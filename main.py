from typing import List
import databases
import sqlalchemy
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pika
import json

DATABASE_URL = "sqlite:///./test.db"

database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

cosmonaut_table = sqlalchemy.Table(
    "cosmonaut_table",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String),
    sqlalchemy.Column("age", sqlalchemy.Integer),
)


engine = sqlalchemy.create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
metadata.create_all(engine)


class CosmonautIn(BaseModel):
    name: str
    age: int


class Cosmonaut(BaseModel):
    id: int
    name: str
    age: int


app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.get("/cosmonauts/", response_model=List[Cosmonaut])
async def read_cosmonauts():
    query = cosmonaut_table.select()
    return await database.fetch_all(query)


@app.post("/cosmonauts/create", response_model=Cosmonaut)
async def create_cosmonaut(cosmonaut: CosmonautIn):
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    channel.queue_declare(queue="kosmonaut_queue", durable=True)
    query = cosmonaut_table.insert().values(
        name=cosmonaut.name,
        age=cosmonaut.age
    )
    last_record_id = await database.execute(query)
    message = {
        "name": cosmonaut.name,
        "age": cosmonaut.age
    }
    
    # RabbitMQ sent message to queue 
    channel.basic_publish(
        exchange="",
        routing_key="kosmonaut_queue",
        body=json.dumps(message)
    )
    
    # RabbitMQ close connection
    connection.close()
    
    return {"id": str(last_record_id), **cosmonaut.dict()}


@app.delete("/cosmonauts/{cosmonaut_id}", response_model=dict)
async def delete_cosmonaut(cosmonaut_id: int):
    query = cosmonaut_table.delete().where(cosmonaut_table.c.id == cosmonaut_id)
    result = await database.execute(query)
    if result:
        return {"message": "Cosmonaut deleted successfully"}
    else:
        raise HTTPException(status_code=404, detail="Cosmonaut not found")


@app.put("/cosmonauts/{cosmonaut_id}", response_model=Cosmonaut)
async def update_cosmonaut(cosmonaut_id: int, updated_cosmonaut: CosmonautIn):
    query = cosmonaut_table.update().where(cosmonaut_table.c.id == cosmonaut_id).values(
        name=updated_cosmonaut.name,
        age=updated_cosmonaut.age
    )
    result = await database.execute(query)
    if result:
        return {"id": cosmonaut_id, **updated_cosmonaut.dict()}
    else:
        raise HTTPException(status_code=404, detail="Cosmonaut not found")
    
    