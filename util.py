from dotenv import dotenv_values
import boto3
import pandas
import csv
env = dotenv_values('.env')

class S3:
    def connect_s3():
        try:
            return boto3.resource(
        service_name=env.get('SERVICE_NAME'),
        region_name=env.get('REGION_NAME'),
        aws_access_key_id=env.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=env.get('AWS_SECRET_ACCESS_KEY')
        )
        except Exception as e:
            print('Cannot connect to S3 due to:' + str(e))
            return None

    def list_all_buckets_s3():
        s3 = S3.connect_s3()
        if s3:
            for bucket in s3.buckets.all():
                print(bucket.name)
        else:
            print("Cannot list buckets")

    def upload_to_bucket_s3(bucket_name,file_path, file_name, file_key=None):
        s3 = S3.connect_s3()
        file_key = file_key if file_key else file_name
        try:
            print('Sending to bucket: ' + bucket_name)
            s3.Bucket(bucket_name).upload_file(Filename=file_path, Key=file_key)
        except Exception as e:
            print('Cannot send to bucket ' + bucket_name + ' due error: ' + str(e))
        finally:
            print(file_name + ' sent successfully to bucket ' + bucket_name)

    def list_all_objects_s3(bucket_name):
        s3 = S3.connect_s3()
        try:
            for obj in s3.Bucket(bucket_name).objects.all():
                print(obj)
        except Exception as e:
            print('Cannot list objects of ' + bucket_name + ' due error: ' + str(e))
        finally:
            print('All objects have been listed')

class Athena:
    def s3_cleanup(bucket_name):
        s3 = S3.connect_s3()
        bucket = s3.Bucket(bucket_name)
        for obj in bucket.objects.filter(Prefix='Query-Results/'):
            s3.Object(bucket_name,obj.key).delete()

    def query_results(session, params, wait = True):
        client = session.client('athena')

        def get_var_char_values(d):
            return [obj.get('VarCharValue') for obj in d['Data']]

        response_query_execution_id = client.start_query_execution(
            QueryString = params['query'],
            QueryExecutionContext = {
                'Database' : "default"
            },
            ResultConfiguration = {
                'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
            }
        )

        if not wait:
            return response_query_execution_id['QueryExecutionId']
        else:
            response_get_query_details = client.get_query_execution(
                QueryExecutionId = response_query_execution_id['QueryExecutionId']
            )
            status = 'RUNNING'
            iterations = 360 # 30 mins
            while (iterations > 0):
                iterations = iterations - 1
                response_get_query_details = client.get_query_execution(
                QueryExecutionId = response_query_execution_id['QueryExecutionId']
                )
                status = response_get_query_details['QueryExecution']['Status']['State']

                if (status == 'FAILED') or (status == 'CANCELLED') :
                    return False, False

                elif status == 'SUCCEEDED':
                    location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']

                    ## Function to get output results
                    response_query_result = client.get_query_results(
                        QueryExecutionId = response_query_execution_id['QueryExecutionId']
                    )

                    if len(response_query_result['ResultSet']['Rows']) > 1:
                        header = response_query_result['ResultSet']['Rows'][0]
                        rows = response_query_result['ResultSet']['Rows'][1:]

                        header = [obj['VarCharValue'] for obj in header['Data']]
                        result = [dict(zip(header, get_var_char_values(row))) for row in rows]
                        delta = response_get_query_details.get('QueryExecution',{}).get('Statistics',{}).get('TotalExecutionTimeInMillis')
                        return location, result, delta
                    else:
                        return location, None, 0
            else:
                    time.sleep(5)

            return False

    def query_athena(query, limit=10):
        params = {
        'region': env.get('REGION_NAME'),
        'database': env.get('DATABASE'),
        'bucket': env.get('BUCKET'),
        'path': env.get('PATH')
    }
        params['query'] = query
        session = boto3.Session()
        location, data, delta = Athena.query_results(session, params)
        print('Locations: ',location)
        print('Execution Time(ms): ' + str(delta))
        print('Result Data(First {}): '.format(limit))
        for d in data[0:limit]:
            print(d)
        # Athena.s3_cleanup(env.get('bucket'))
