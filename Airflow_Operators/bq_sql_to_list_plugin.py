# Modified BigQuery Operator to return query results as a Python List that can be used by other operators in your DAG.
# Details on adding this to your Cloud Composer instance can be found at the link below. We have found it is 
# best to use the gcloud command as opposed to simply uploading to your bucket. 
# https://cloud.google.com/composer/docs/concepts/plugins


# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This module contains Google BigQuery operators.
"""

import json

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook, _parse_gcs_url
from airflow.models import BaseOperator
from airflow.models import TaskInstance
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

# pylint: disable=too-many-instance-attributes
class BigQuerySQLToListOperator(BaseOperator):
    """
    Executes BigQuery SQL queries in a specific BigQuery database

    :param sql: the sql code to be executed (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'.
    :param destination_dataset_table: A dotted
        ``(<project>.|<project>:)<dataset>.<table>`` that, if set, will store the results
        of the query. (templated)
    :type destination_dataset_table: str
    :param write_disposition: Specifies the action that occurs if the destination table
        already exists. (default: 'WRITE_EMPTY')
    :type write_disposition: str
    :param create_disposition: Specifies whether the job is allowed to create new tables.
        (default: 'CREATE_IF_NEEDED')
    :type create_disposition: str
    :param allow_large_results: Whether to allow large results.
    :type allow_large_results: bool
    :param flatten_results: If true and query uses legacy SQL dialect, flattens
        all nested and repeated fields in the query results. ``allow_large_results``
        must be ``true`` if this is set to ``false``. For standard SQL queries, this
        flag is ignored and results are never flattened.
    :type flatten_results: bool
    :param bigquery_conn_id: reference to a specific BigQuery hook.
    :type bigquery_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param udf_config: The User Defined Function configuration for the query.
        See https://cloud.google.com/bigquery/user-defined-functions for details.
    :type udf_config: list
    :param use_legacy_sql: Whether to use legacy SQL (true) or standard SQL (false).
    :type use_legacy_sql: bool
    :param maximum_billing_tier: Positive integer that serves as a multiplier
        of the basic price.
        Defaults to None, in which case it uses the value set in the project.
    :type maximum_billing_tier: int
    :param maximum_bytes_billed: Limits the bytes billed for this job.
        Queries that will have bytes billed beyond this limit will fail
        (without incurring a charge). If unspecified, this will be
        set to your project default.
    :type maximum_bytes_billed: float
    :param api_resource_configs: a dictionary that contain params
        'configuration' applied for Google BigQuery Jobs API:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
        for example, {'query': {'useQueryCache': False}}. You could use it
        if you need to provide some params that are not supported by BigQueryOperator
        like args.
    :type api_resource_configs: dict
    :param schema_update_options: Allows the schema of the destination
        table to be updated as a side effect of the load job.
    :type schema_update_options: tuple
    :param query_params: a list of dictionary containing query parameter types and
        values, passed to BigQuery. The structure of dictionary should look like
        'queryParameters' in Google BigQuery Jobs API:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs.
        For example, [{ 'name': 'corpus', 'parameterType': { 'type': 'STRING' },
        'parameterValue': { 'value': 'romeoandjuliet' } }].
    :type query_params: list
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :type labels: dict
    :param priority: Specifies a priority for the query.
        Possible values include INTERACTIVE and BATCH.
        The default value is INTERACTIVE.
    :type priority: str
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and expiration as per API specifications.
    :type time_partitioning: dict
    :param cluster_fields: Request that the result of this query be stored sorted
        by one or more columns. This is only available in conjunction with
        time_partitioning. The order of columns given determines the sort order.
    :type cluster_fields: list[str]
    :param location: The geographic location of the job. Required except for
        US and EU. See details at
        https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :type location: str
    """

    template_fields = ('sql', 'destination_dataset_table', 'labels')
    template_ext = ('.sql', )
    ui_color = '#e4f0e8'

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(self,
                 sql,
                 sort_by=None,
                 destination_dataset_table=None,
                 write_disposition='WRITE_EMPTY',
                 allow_large_results=False,
                 flatten_results=None,
                 bigquery_conn_id='google_cloud_default',
                 delegate_to=None,
                 udf_config=None,
                 use_legacy_sql=True,
                 maximum_billing_tier=None,
                 maximum_bytes_billed=None,
                 create_disposition='CREATE_IF_NEEDED',
                 schema_update_options=(),
                 query_params=None,
                 labels=None,
                 priority='INTERACTIVE',
                 time_partitioning=None,
                 api_resource_configs=None,
                 cluster_fields=None,
                 location=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.sort_by = sort_by
        self.destination_dataset_table = destination_dataset_table
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.allow_large_results = allow_large_results
        self.flatten_results = flatten_results
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.udf_config = udf_config
        self.use_legacy_sql = use_legacy_sql
        self.maximum_billing_tier = maximum_billing_tier
        self.maximum_bytes_billed = maximum_bytes_billed
        self.schema_update_options = schema_update_options
        self.query_params = query_params
        self.labels = labels
        self.bq_cursor = None
        self.priority = priority
        self.time_partitioning = time_partitioning
        self.api_resource_configs = api_resource_configs
        self.cluster_fields = cluster_fields
        self.location = location

    def execute(self, context):
        if self.bq_cursor is None:
            self.log.info('Executing: %s', self.sql)
            hook = BigQueryHook(
                bigquery_conn_id=self.bigquery_conn_id,
                use_legacy_sql=self.use_legacy_sql,
                delegate_to=self.delegate_to,
                location=self.location,
            )
            conn = hook.get_conn()
            self.bq_cursor = conn.cursor()
        job_id = self.bq_cursor.run_query(
            sql=self.sql,
            destination_dataset_table=self.destination_dataset_table,
            write_disposition=self.write_disposition,
            allow_large_results=self.allow_large_results,
            flatten_results=self.flatten_results,
            udf_config=self.udf_config,
            maximum_billing_tier=self.maximum_billing_tier,
            maximum_bytes_billed=self.maximum_bytes_billed,
            create_disposition=self.create_disposition,
            query_params=self.query_params,
            labels=self.labels,
            schema_update_options=self.schema_update_options,
            priority=self.priority,
            time_partitioning=self.time_partitioning,
            api_resource_configs=self.api_resource_configs,
            cluster_fields=self.cluster_fields,
        )
        context['task_instance'].xcom_push(key='job_id', value=job_id)
        
        df = hook.get_pandas_df(
            self.sql
            )
        
        if self.sort_by is not None:
            df.sort_values('self.sort_by')
        
        list_to_return = df.astype(str).to_dict('index')
        print(list_to_return)
        return list_to_return
        

    def on_kill(self):
        super().on_kill()
        if self.bq_cursor is not None:
            self.log.info('Cancelling running query')
            self.bq_cursor.cancel_query()


class BQSQLToList(AirflowPlugin):
    name = "bq_sql_to_list_plugin"
    operators = [BigQuerySQLToListOperator]
    # A list of class(es) derived from BaseHook
    hooks = []
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []