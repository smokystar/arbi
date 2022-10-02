from aiogram.utils import executor
from create import dp


async def on_startup(_):
    print('Bot is online')

import commands
import clients

commands.register_handlers_client_partners(dp)
# clients.register_handlers(dp)


executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
