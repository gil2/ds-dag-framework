def generate_dataset(run_type, table, features, xcom):
    import time

    import typing as t
    from google.protobuf.json_format import ParseDict
    from pyhive.exc import DatabaseError
    from wix.ds.dataset.v1.dataset_generation_job_pb2 import DatasetGenerationJob, DatasetJobState
    from wix_trino_client.trino_connection import WixTrinoConnection
    from wixml_clients.dataset_generator import DatasetGenerator, QueryDatasetGenerationJobRequest, \
        GenerateDatasetRequest, QueryDatasetGenerationJobResponse

    def trigger(run_type: str, table: str, features: str, mapping: t.Dict[str, str]) -> str:
        dataset_generator = DatasetGenerator()
        features = features.split(',')
        table_name = table.split('.')[-1]
        request: QueryDatasetGenerationJobRequest = build_request(run_type=run_type, features=features,
                                                                  dataset_name=table_name, kernel=table)
        if table_name in mapping:
            if query_job(dataset_generator, mapping[table_name]).job_state in {DatasetJobState.TRIGGERED,
                                                                               DatasetJobState.RUNNING}:
                return mapping[table_name]
            if not query_job(dataset_generator, mapping[table_name]).job_state == DatasetJobState.END_SUCCESS:
                print(f"making dsg request for kernel {table_name}")
                dataset = dataset_generator.generate_dataset_base(request)
                print(f"Dataset generation requested: {dataset.job_id}")
                return dataset.job_id
            return mapping[table_name]
        dataset = dataset_generator.generate_dataset_base(request)
        print(f"Dataset generation requested: {dataset.job_id}")
        return dataset.job_id

    def write_job_ids(job_ids, xcom):
        pc = WixTrinoConnection(username='wix')
        try:
            pc.write_data(table_name=xcom, data=[(job_id,) for job_id in job_ids], col_types=['varchar'],
                          col_names=['job_id'], if_exists='replace')
        except Exception as e:
            print(e)
            time.sleep(120)
            pc.write_data(table_name=xcom, data=[(job_id,) for job_id in job_ids], col_types=['varchar'],
                          col_names=['job_id'], if_exists='replace')

    def get_job_ids(xcom):
        pc = WixTrinoConnection(username='wix')
        job_mapping = {}
        try:
            jobs = pc.execute_sql(f"select * from {xcom}")
        except DatabaseError as e:
            print(e)
            return {}
        jobs = [job[0] for job in jobs]
        dsg = DatasetGenerator()
        for job in jobs:
            dataset = query_job(dsg, job)
            name = dataset.dataset_name
            job_mapping[name] = job
        return job_mapping

    def build_request(run_type: str, features: t.List[str], dataset_name: str, kernel: str) -> GenerateDatasetRequest:
        req_dict = {'dataset_name': dataset_name,
                    'features_names': features,
                    'kernel': kernel,
                    'project_id': '87ffb666-fec6-4e97-ab5e-be820759d0e1',
                    'description': 'daily site classification enrichment'}
        if run_type == 'backfill':
            req_dict['minimum_date'] = '2017-01-01T00:00:00Z'
        request = ParseDict(req_dict, GenerateDatasetRequest())
        return request

    def query_job(client: DatasetGenerator, job_id: str) -> DatasetGenerationJob:
        query = ParseDict({"query": {"filter": {"id": {"$eq": job_id}}}}, QueryDatasetGenerationJobRequest())
        query_response: QueryDatasetGenerationJobResponse = client.query_jobs(query)
        job: DatasetGenerationJob = query_response.jobs[0]
        return job

    def main(run_type, table, features, xcom):
        tables = table.split(',')
        job_id_mapping = get_job_ids(xcom)
        job_ids = []
        for table in tables:
            job_ids.append(trigger(run_type, table, features, mapping=job_id_mapping))
        write_job_ids(job_ids, xcom)

    main(run_type, table, features, xcom)
