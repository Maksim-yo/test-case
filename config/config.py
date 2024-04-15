from dataclasses import dataclass
import os
from pathlib import Path

from db.MongoDB import MongoDB
from db.dao import DAO
from exceptions.exceptions import TokenException

@dataclass
class Token:
    token: str


@dataclass
class Config:
    token: Token
    db_connection_string: str = "mongodb://127.0.0.1:12017"
    db_config_file: str = (Path(__file__).parent.parent / "sample_collection.metadata.json").resolve()
    db_data_file: str = (Path(__file__).parent.parent / "sample_collection.bson").resolve()


dao = None


async def init_db(connection_string: str) -> MongoDB:
    db = MongoDB(connection_string)
    await db.connect()
    return db


def init_dao(db: MongoDB) -> DAO:
    global dao
    dao = DAO(db)
    return dao


def load_config(path: str | None = None) -> Config:
    try:
        token_key = os.environ['BOT_API_KEY']
    except KeyError:
        raise TokenException("Cannot find BOT_API_KEY. Please provide it via env")

    return Config(token=Token(token=token_key), db_connection_string="mongodb://127.0.0.1:27017")


