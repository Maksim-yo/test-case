import motor.motor_asyncio
from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorClient, AsyncIOMotorDatabase
from typing import List


class MongoDB:

    client: AsyncIOMotorClient = None
    db: AsyncIOMotorDatabase = None
    collection: AsyncIOMotorCollection = None
    connection_string = ""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    async def connect(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(self.connection_string, serverSelectionTimeoutMS=5000)
        try:
            await self.client.admin.command("ismaster")
        except Exception:
            pass

    async def disconnect(self):
        self.client.close()

    def get_or_create_database(self, db_name: str | None = None) -> AsyncIOMotorDatabase:
        if db_name is None:
            return self.db
        self.db = self.client[db_name]
        return self.db

    def get_or_create_collection(self, collection_name: str | None = None) -> AsyncIOMotorCollection:
        if collection_name is None:
            return self.collection
        self.collection = self.db[collection_name]
        return self.collection

    async def insert_documents(self, data):
        if isinstance(data, List):
            await self.collection.insert_many(data)
        else:
            await self.collection.insert_one(data)

    def find(self, *args, **kwargs):
        return self.collection.find(*args, **kwargs)