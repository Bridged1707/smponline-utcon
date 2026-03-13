async def create_account(conn, discord_uuid, mc_uuid, mc_name):

    await conn.execute(
        """
        INSERT INTO accounts(discord_uuid, mc_uuid, mc_name)
        VALUES($1,$2,$3)
        """,
        discord_uuid,
        mc_uuid,
        mc_name,
    )

    await conn.execute(
        """
        INSERT INTO balances(discord_uuid, balance)
        VALUES($1,0)
        """,
        discord_uuid,
    )