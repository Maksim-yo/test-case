import asyncio
from aiogram import Bot, Dispatcher
from handlers import callbacks, commands
from config.config import init_db, init_dao, load_config, Config




async def create_database(config: Config):
    db = await init_db(config.db_connection_string)
    dao = init_dao(db)
    await dao.configure(config.db_config_file, config.db_data_file)


async def main():
    config = load_config()
    await create_database(config)
    bot = Bot(token=config.token.token)
    dp = Dispatcher()
    dp.include_router(commands.router)
    dp.include_router(callbacks.router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())