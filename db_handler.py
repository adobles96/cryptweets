import psycopg2 as pg

class Handler:

    tweet_table_template = '(id_str VARCHAR(20) NOT NULL, created_at TIME WITH TIME ZONE, text TEXT NOT NULL, user TEXT NOT NULL, retweets INTEGER, coins TEXT, language TEXT, lat REAL, long REAL, witheld_in_countries TEXT)'
    # Possibly useful in the future: to check an existing table's template use the following query: SELECT (column_name, data_type) FROM information_schema.columns WHERE table_name = 'times_test';

    def __init__(self, db_name, user, password='', host='localhost'):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.cur = self.con = None

    def __enter__(self):
        try:
            self.con = pg.connect(dbname=self.db_name, user=self.user, password=self.password, host=self.host) #will possibly need to include host = 'localhost'
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
        self.con.commit() # This saves all changes made during the current context
        self.cur.close()
        self.con.close()

    def cursor_is_active(self):
        if self.cur:
            return not self.cur.closed
        return False

    def create_table(self, table_name, table_template):
        '''Creates a table with the specified name and with columns as in the
        variable TWEET_TABLE_TEMPLATE.'''
        assert(self.cursor_is_active())
        try:
            self.cur.execute('CREATE TABLE {} {};'.format(table_name, table_template))
            self.con.commit()
        except pg.Error as e:
            print('Table creation failed. Rolling back connection. ERROR:', e)
            # resets cursor, otherwise any future executes will generate an InternalError
            self.con.rollback()

    def drop_table(self, table_name):
        '''Drops the table with the specified name.'''
        assert(self.cursor_is_active())
        try:
            self.cur.execute('DROP TABLE {};'.format(table_name))
            self.con.commit()
        except pg.Error as e:
            print('Failed to drop table. Rolling back connection. ERROR:', e)
            # resets cursor, otherwise any future executes will generate an InternalError
            self.con.rollback()

    # def unpack_tweet(tweet):
    #     '''Receives a tweet and unpacks the relevant information from the json.
    #     It then returns a VALUES string in valid SQL format. IF ANY CHANGES ARE
    #     MADE TO THE TWEETS TABLE BE SURE TO CHANGE THIS METHOD!'''

    def insert_tweet(self, table_name, tweet, unpacker):
        '''Uses the unpacker function to unpack the tweet (ie to take information
        in tweet and put it in the proper SQL format) and inserts that into the
        table specified by table name. It is important that the unpacker outputs
        values in a format that matches the columns of the specified table.
        IMPORTANT: changes to the db won't be commited until the context closes.
        this means you won't be able to read what you've written within the same context.
        To change this there's an easy but costly fix. Simply add self.con.commit()
        after the execute statement.'''
        assert(self.cursor_is_active())

        try:
            self.cur.execute('INSERT INTO {} VALUES {}'.format(table_name, unpacker(tweet)))
        except pg.Error as e:
            print('Failed to insert tweet. Rolling back connection. ERROR:', e)
            # resets cursor, otherwise any future executes will generate an InternalError
            self.con.rollback()

    def retrieve_tweets(self, table_name): # This function might be superfluous, leaving it in nonetheless
        '''A generator that retrieves all tweets in the specified table.'''
        try:
            self.cur.execute('SELECT * FROM {};'.format(table_name))
        except pg.Error as e:
            print('Failed to retrieve tweets. Rolling back connection. ERROR:', e)
            # resets cursor, otherwise any future executes will generate an InternalError
            self.con.rollback()
        for result in self.cur:
             yield result

    def query(self, query): # This function might be a little dangerous, leaving it in nonetheless
        '''A generator that executes whatever query and yields the esults one by
        one. IMPORTANT: changes to the db won't be commited until the context closes.
        this means you won't be able to read what you've written within the same context.
        To change this there's an easy but costly fix. Simply add self.con.commit()
        after the execute statement.'''
        try:
            self.cur.execute(query)
        except pg.Error as e:
            print('Query failed. Rolling back connection. ERROR:', e)
            # resets cursor, otherwise any future executes will generate an InternalError
            self.con.rollback()
        for result in self.cur:
            yield result
