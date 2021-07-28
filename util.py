from dotenv import dotenv_values
import boto3
import pandas as pd
import csv
import time
import progressbar
env = dotenv_values('.env')

class S3:
    def client_s3():
        try:
            return boto3.client(
        service_name=env.get('SERVICE_NAME'),
        region_name=env.get('REGION_NAME'),
        aws_access_key_id=env.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=env.get('AWS_SECRET_ACCESS_KEY')
        )
        except Exception as e:
            print('Cannot connect to S3 due to:' + str(e))
            return None

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

    def download_file_s3(bucket_name, file_name):
        s3 = S3.connect_s3()
        s3_client = S3.client_s3()
        size = s3_client.head_object(Bucket=bucket_name, Key='output/' + file_name).get('ContentLength')
        print('Starting download:')
        progress = progressbar.progressbar.ProgressBar(maxval=size)
        progress.start()
        def download_progress(chunk):
            progress.update(progress.currval + chunk)

        s3.Bucket(bucket_name).download_file('output/' + file_name, 'downloads/' + file_name, Callback=download_progress)
        progress.finish()

class Athena:
    # def s3_cleanup(bucket_name):
    #     s3 = S3.connect_s3()
    #     bucket = s3.Bucket(bucket_name)
    #     for obj in bucket.objects.filter(Prefix='Query-Results/'):
    #         s3.Object(bucket_name,obj.key).delete()

    def query_results(session, params, wait = True):
        client = session.client('athena')

        def get_var_char_values(d):
            return [obj.get('VarCharValue') for obj in d['Data']]

        response_query_execution_id = client.start_query_execution(
            WorkGroup="primary",
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
                total_execution = response_get_query_details['QueryExecution']['Statistics']['TotalExecutionTimeInMillis']
                print('Athena execution time: ' + str(total_execution) + 'ms', end='\r')

                if (status == 'FAILED') or (status == 'CANCELLED') :
                    return 'False', 'False'

                elif status == 'SUCCEEDED':
                    location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']

                    ## Function to get output results
                    response_query_result = client.get_query_results(
                        QueryExecutionId = response_query_execution_id['QueryExecutionId']
                    )

                    if len(response_query_result['ResultSet']['Rows']) > 1:
                        header = response_query_result['ResultSet']['Rows'][0]

                        header = [obj['VarCharValue'] for obj in header['Data']]
                        delta = response_get_query_details.get('QueryExecution',{}).get('Statistics',{}).get('TotalExecutionTimeInMillis')
                        return location, delta
                    else:
                        return location, 0
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
        location, delta = Athena.query_results(session, params)
        print('Athena execution time: ' + str(delta) + 'ms')
        print('Location: ',location)

        return location, delta
        # Athena.s3_cleanup(env.get('bucket'))
