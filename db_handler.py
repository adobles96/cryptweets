import psycopg2 as pg


class Handler:

    # tweet_table_template = '(id_str VARCHAR(20) NOT NULL, created_at TIME WITH TIME ZONE, text TEXT NOT NULL, user TEXT NOT NULL, retweets INTEGER, coins TEXT, language TEXT, lat REAL, long REAL, witheld_in_countries TEXT)'
    # Possibly useful in the future: to check an existing table's template use the following query: SELECT (column_name, data_type) FROM information_schema.columns WHERE table_name = 'times_test';

    def __init__(self, db_name, user, password='', host='localhost'):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.cur = self.con = None

    def __enter__(self):
        try:
            self.con = pg.connect(dbname=self.db_name, user=self.user, password=self.password, host=self.host)  # will possibly need to include host = 'localhost'
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

    def insert_tweet(self, table_name, tweet, unpacker):
        '''Uses the unpacker function to unpack the tweet (ie to take information
        in tweet and put it in the proper SQL format) and inserts that into the
        table specified by table name. It is important that the unpacker outputs
        values in a format that matches the columns of the specified table.
        IMPORTANT: this method depends on table design!'''
        assert(self.cursor_is_active())
        original = tweet.get('retweeted_status', None)
        if original: # if original not None then tweet is a RT
            result = query_list("SELECT retweets FROM {} WHERE id_str='{}';".format(table_name, original[id_str]))
            if result:
                # This means the tweet is already in our db. We must increase it's RT count
                try:
                    query_list("UPDATE {} SET retweets={} WHERE id_str='{}';".format(table_name, result[0][0]+1, original[id_str]))
                    return
                except pg.Error as e:
                    print('Failed to update RT count. Rolling back connection. ERROR:', e)
                    self.con.rollback()
            else:
                insert_tweet(table_name, original, unpacker) # RECURSIVE DANGER! PROCEED WITH CAUTION!
        try:
            # self.cur.execute('INSERT INTO {} VALUES {}'.format(table_name, unpacker(tweet)))
            self.cur.execute('INSERT INTO {} VALUES {}'.format(table_name, unpacker(tweet)[0]), tuple(unpacker(tweet)[1]))
            self.con.commit()
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

    def query_list(self, query):
        '''Executes a query and resturns the output as a list.'''
        try:
            self.cur.execute(query)
            self.con.commit()
            if self.cur.description:
                return self.cur.fetchall()
        except pg.Error as e:
            print('Query failed. Rolling back connection. ERROR:', e)
            # resets cursor, otherwise any future executes will generate an InternalError
            self.con.rollback()


    def query_generator(self, query, generator=False): # This function might be a little dangerous, leaving it in nonetheless
        '''A generator that executes whatever query and yields the results one by
        one.'''
        try:
            self.cur.execute(query)
            self.con.commit()
            for result in self.cur:
                yield result
        except pg.Error as e:
            print('Query failed. Rolling back connection. ERROR:', e)
            # resets cursor, otherwise any future executes will generate an InternalError
            self.con.rollback()
