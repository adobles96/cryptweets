import psycopg2 as pg

TWEET_TABLE_TEMPLATE = '(id_str VARCHAR(20), date_created DATE, text TEXT, user )'


class Handler:
    def __init__(self, db_name, user, password):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.cur = None
        self.con = None

    def __enter__(self):
        try:
            self.con = pg.connect(dbname=self.db_name, user=self.user, password=self.password)
            self.cur = self.con.cursor()
        except pg.Error as e:
            # Perhaps I shouldn't except this error
            print('Failed to establish connection to the database. ERROR:', e)
        # return self
        # uncomment line above to enable using handler in the following manner:
        # with db_handler.Handler(...) as handler:
        # otherwise the handler should be used as follows:
        # handler = db_handler.Handler(...)
        # with handler:

    def __exit__(self, *exc):
        self.cur.close()
        self.con.close()

    def cursor_is_active(self):
        if self.cur:
            return not self.cur.closed
        return False

    def create_table(self, table_name):
        '''Creates a table with the specified name and with columns as in the
        variable TWEET_TABLE_TEMPLATE.'''
        assert(self.cursor_is_active())
        try:
            self.cur.execute('CREATE TABLE {} {};'.format(table_name, TWEET_TABLE_TEMPLATE))
        except pg.Error as e:
            print('Table creation failed. Rolling back connection. ERROR:', e)
            # resets cursor, otherwise any future executes will generate an InternalError
            self.con.rollback()

    def drop_table(self, table_name):
        '''Drops the table with the specified name.'''
        assert(self.cursor_is_active())
        try:
            self.cur.execute('DROP TABLE {};'.format(table_name))
        except pg.Error as e:
            print('Failed to drop table. Rolling back connection. ERROR:', e)
            # resets cursor, otherwise any future executes will generate an InternalError
            self.con.rollback()

    def insert_tweet(self, table_name):
        assert(self.cursor_is_active())
        pass

    def retrieve_days_tweets(self, table_name):
        pass
        # GENERATOR!

    def retrieve_query(self, query):
        pass
