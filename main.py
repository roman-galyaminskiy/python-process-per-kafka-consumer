from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from config import Config
from kafka_binding_processes import KafkaBindingProcesses


# from kafka_binding_executor import KafkaBindingExecutor
# from kafka_binding_processes import KafkaBindingProcesses as KafkaBinding


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("lifespan startup")
    binding = KafkaBindingProcesses(config.binding.kafka)
    await binding.on_startup()
    yield
    print("lifespan shutdown")
    await binding.on_shutdown()

app = FastAPI(lifespan=lifespan)
config = Config()





@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8080,
        workers=1,
    )
