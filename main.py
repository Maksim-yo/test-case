import json
import asyncio
from aiogram import Bot, Dispatcher
from aiogram.utils.callback_answer import CallbackAnswerMiddleware
from handlers import callbacks, commands

from db.dao import DAO




async def main():
    bot = Bot(token=API_TOKEN)
    dp = Dispatcher()
    dp.include_router(commands.router)
    dp.include_router(callbacks.router)
    dp.startup.register(on_startup)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())