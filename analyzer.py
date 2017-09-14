def coin_count(handler, table_name):
    btc_count = handler.query_list("SELECT SUM(retweets) FROM {} WHERE coins LIKE '%btc%'".format(table_name))[0][0]
    eth_count = handler.query_list("SELECT SUM(retweets) FROM {} WHERE coins LIKE '%eth%'".format(table_name))[0][0]
    return '{} people tweeted about Ethereum in the past 24 hours. #eth #crypto'.format(eth_count), '{} people tweeted about Bitcoin in the past 24 hours. #btc #crypto'.format(btc_count)
