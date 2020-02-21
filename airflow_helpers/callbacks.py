import json

from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook


class _SlackFailureHook(HttpHook):
    def __init__(self, webhook_token=None, *args, **kwargs):
        super(_SlackFailureHook, self).__init__(*args, **kwargs)

        self.webhook_token = self._get_token(webhook_token)


    def _get_token(self, token):
        if token:
            return token

        if self.http_conn_id:
            conn = self.get_connection(self.http_conn_id)
            webhook_token = conn.extra_dejson.get('webhook_token')
            if webhook_token:
                return webhook_token

        raise AirflowException('Cannot get token: No valid Slack '
                               'webhook token nor conn_id supplied')


    def run(self, ti):
        msg_text = 'Airflow Task Failed'
        msg_color = '#ff0000'
        msg_fields = [{"title": "Dag",
                       "value": ti.dag_id,
                       "short": True},
                      {"title": "Task",
                       "value": ti.task_id,
                       "short": True},
                      {"title": "Execution Date",
                       "value": str(ti.execution_date.in_tz('Asia/Taipei')),
                       "short": True},
                      {"title": "Log",
                       "value": ti.log_url,
                       "short": False}]

        data = {"text": msg_text,
                "attachments": [{"color": msg_color,
                                 "fields": msg_fields}]}
        data_str = json.dumps(data)

        super(_SlackFailureHook, self).run(
                endpoint=self.webhook_token,
                data=data_str,
                headers={'Content-type': 'application/json'})


def failure_callback(context, http_conn_id='http_default'):
    ti = context['ti']

    hook = _SlackFailureHook(http_conn_id=http_conn_id)
    return hook.run(ti)
