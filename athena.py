import util
import time

from dotenv import dotenv_values

env = dotenv_values('.env')

# util.S3.list_all_buckets_s3()
# util.S3.upload_to_bucket_s3(env.get('BUCKET'), 'resources/prices.csv', 'prices/prices.csv')
# util.S3.list_all_objects_s3('mmzanes-athena')

# query_max_open = '''SELECT symbol, max(open) AS max_open FROM "{}"."prices" GROUP BY symbol '''.format(env.get('DATABASE'))
# location, results, delta = util.Athena.query_athena(query_max_open)

# util.Reports.generate_report(location, results)

start = time.time()
query_all = '''SELECT * FROM {}.prices '''.format(env.get('DATABASE'))
location, delta = util.Athena.query_athena(query_all)
util.S3.download_file_s3(env.get('BUCKET'), str(location.split('/')[-1]))
end = time.time()
print('Time to complete : {}ms'.format((end - start)*1000))
