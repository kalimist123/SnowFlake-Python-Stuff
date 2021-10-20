import asyncio

async def waiter():
    task1 = asyncio.create_task(cook('Pasta', 8))
    task2 = asyncio.create_task(cook('Caesar Salad', 3))
    task3 = asyncio.create_task(cook('Lamb Chops', 16))

    await task1
    await task2
    await task3

async def cook(order, time_to_prepare):
    print(f'Getting {order} order')
    await asyncio.sleep(time_to_prepare)
    print(order, 'ready')

asyncio.run(waiter())