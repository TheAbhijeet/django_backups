import warnings
from types import SimpleNamespace

from django.conf import settings
from django.db import (
    DEFAULT_DB_ALIAS,
    IntegrityError,
    InternalError,
    OperationalError,
    connections,
    transaction,
)
from django.utils.timezone import now


def get_db_connector():
    # Determine the database type
    database_engine = settings.DATABASES[DEFAULT_DB_ALIAS]["ENGINE"]

    # Create a backup based on the database type
    if "sqlite3" in database_engine:
        return SqliteConnector()
    elif "postgresql" in database_engine:
        return PostgresConnector()
    else:
        raise Exception(
            f"Database type '{database_engine}' is not supported for backup."
        )


class BaseDBConnector:
    def __init__(self):
        self.backup_root = settings.BACKUP_ROOT
        self.media_root = settings.MEDIA_ROOT
        self.connection = connections[DEFAULT_DB_ALIAS]
        self.exclude_tables = ["backups_restore"]
        self.backup_path = self.get_backup_path()

    @staticmethod
    def get_relative_media_file_path(absolute_file_path):
        # Get the relative file path by removing the MEDIA_ROOT prefix
        relative_file_path = str(absolute_file_path.relative_to(settings.MEDIA_ROOT))
        # Normalize the path separators
        relative_file_path = relative_file_path.replace("\\", "/")
        return relative_file_path

    def create_backup(self):
        if not self.connection.is_usable():
            self.connection.connect()
        with open(self.backup_path, "w") as f:
            self._write_dump(f)
        return self.get_relative_media_file_path(self.backup_path)

    def restore_backup(self, backup_file):
        raise NotImplementedError

    def get_backup_path(self):
        timestamp = now().strftime("%d-%m-%Y-%H::%M")
        backup_filename = f"backup_{timestamp}.sql"
        return self.backup_root / backup_filename

    @staticmethod
    def run_sql(sql_file, cursor):
        for line in sql_file.readlines():
            try:
                with transaction.atomic():
                    cursor.execute(line.strip().decode("UTF-8"))
            except (OperationalError, IntegrityError) as err:
                warnings.warn(f"Error in db restore: {err}")

    def _write_dump(self, file_obj):
        raise NotImplementedError

    def is_excluded_table(self, table_name):
        excluded_prefixes = ["django_", "auth_", "sqlite_"]
        return (
            table_name.startswith(tuple(excluded_prefixes))
            or table_name in self.exclude_tables
        )


class PostgresConnector(BaseDBConnector):
    def __init__(self):
        db = settings.DATABASES[DEFAULT_DB_ALIAS]
        self.db_host = db["HOST"]
        self.db_port = db["PORT"] or "5432"
        self.db_name = db["NAME"]
        self.db_user = db["USER"]
        self.db_password = db["PASSWORD"]
        super().__init__()

    @staticmethod
    def sql_queries() -> SimpleNamespace:
        sql_queries = SimpleNamespace(
            GET_TABLE_NAME="SELECT tablename FROM pg_tables WHERE schemaname='public'",
            SELECT_TABLE="SELECT * FROM {table_name}",
            INSERT_ROW="INSERT INTO {table_name} VALUES ({values});",
            SET_SEQUENCE="SELECT setval('{sequence_name}', (SELECT MAX(id) FROM {table_name}));",
            SELECT_TABLE_NAMES="SELECT tablename FROM pg_tables WHERE schemaname='public'",
            TRUNCATE_TABLE="TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE",
        )
        return sql_queries

    def _write_dump(self, file_obj):
        cursor = self.connection.connection.cursor()
        sql = self.sql_queries()

        # Get table names
        cursor.execute(sql.GET_TABLE_NAME)
        table_names = cursor.fetchall()

        # Write INSERT statements for each table
        for table_name in table_names:
            if self.is_excluded_table(table_name[0]):
                continue

            # INSERT statements
            cursor.execute(sql.SELECT_TABLE.format(table_name=table_name[0]))
            rows = cursor.fetchall()

            for row in rows:
                insert_query = sql.INSERT_ROW.format(
                    table_name=table_name[0], values=",".join(["%s"] * len(row))
                )
                file_obj.write(cursor.mogrify(insert_query, row).decode() + "\n")

            # Sequence updates
            sequence_name = f"{table_name[0]}_id_seq"
            sequence_query = sql.SET_SEQUENCE.format(
                sequence_name=sequence_name, table_name=table_name[0]
            )
            file_obj.write(sequence_query + "\n")

        cursor.close()

    def restore_backup(self, backup_file):
        if not self.connection.is_usable():
            self.connection.connect()
        cursor = self.connection.cursor()

        sql = self.sql_queries()

        cursor.execute(sql.SELECT_TABLE_NAMES)
        table_names = [row[0] for row in cursor.fetchall()]

        self.clear_tables(table_names, cursor)

        self.run_sql(backup_file, cursor)

    def clear_tables(self, table_names, cursor):
        sql = self.sql_queries()
        # Truncate tables
        for table_name in table_names:
            if self.is_excluded_table(table_name):
                continue
            try:
                cursor.execute(sql.TRUNCATE_TABLE.format(table_name=table_name))
            except (InternalError, OperationalError) as e:
                warnings.warn(
                    f"Ignoring error while truncating table {table_name}: {e}"
                )


class SqliteConnector(BaseDBConnector):
    @staticmethod
    def sql_queries() -> SimpleNamespace:
        sql_queries = SimpleNamespace(
            GET_TABLE_NAME="SELECT tablename FROM pg_tables WHERE schemaname='public'",
            INSERT_ROW="""SELECT 'INSERT INTO "{0}" VALUES({1})' FROM "{0}";\n""",
            TRUNCATE_TABLE="TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE",
            DUMP_TABLES="""
                   SELECT "name", "type", "sql"
                   FROM "sqlite_master"
                   WHERE "sql" NOT NULL AND "type" == 'table'
                   ORDER BY "name"
                   """,
        )
        return sql_queries

    def _write_dump(self, file_obj):
        sql = self.sql_queries()
        cursor = self.connection.connection.cursor()
        cursor.execute(sql.DUMP_TABLES)
        for table_name, _, sql in cursor.fetchall():
            if self.is_excluded_table(table_name):
                continue

            if sql.startswith("CREATE TABLE"):
                sql = sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
                # Make SQL commands in 1 line
                sql = sql.replace("\n    ", "")
                sql = sql.replace("\n)", ")")
            file_obj.write(f"{sql};\n".encode())

            table_name_ident = table_name.replace('"', '""')
            res = cursor.execute(f'PRAGMA table_info("{table_name_ident}")')

            column_names = [str(table_info[1]) for table_info in res.fetchall()]
            q = sql.INSERT_ROW.format(
                table_name_ident,
                ",".join(
                    """'||quote("{}")||'""".format(col.replace('"', '""'))
                    for col in column_names
                ),
            )
            query_res = cursor.execute(q)
            for row in query_res:
                file_obj.write(f"{row[0]};\n".encode())
        cursor.close()

    def restore_backup(self, backup_file):
        if not self.connection.is_usable():
            self.connection.connect()
        cursor = self.connection.cursor()

        # Get the names of all tables except the session table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        table_names = [row[0] for row in cursor.fetchall()]

        self.clear_tables(table_names, cursor)

        self.run_sql(backup_file, cursor)

    def clear_tables(self, table_names, cursor):
        for table_name in table_names:
            if self.is_excluded_table(table_name):
                continue
            cursor.execute(f"DELETE FROM {table_name}")
