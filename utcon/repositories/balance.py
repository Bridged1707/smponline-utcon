async def get_balance(conn, discord_uuid):

    return await conn.fetchval(
        """
        SELECT balance
        FROM balances
        WHERE discord_uuid=$1
        """,
        discord_uuid,
    )


async def add_balance(conn, discord_uuid, amount):

    await conn.execute(
        """
        UPDATE balances
        SET balance = balance + $1
        WHERE discord_uuid=$2
        """,
        amount,
        discord_uuid,
    )


async def subtract_balance(conn, discord_uuid, amount):

    await conn.execute(
        """
        UPDATE balances
        SET balance = balance - $1
        WHERE discord_uuid=$2
        """,
        amount,
        discord_uuid,
    )