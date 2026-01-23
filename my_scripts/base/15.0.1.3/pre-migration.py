from openupgradelib import openupgrade


@openupgrade.migrate()
def migrate(env, version):
    openupgrade.logged_query(
        env.cr, "update res_partner set tz='Europe/Paris' where tz = 'CET';"
    )
