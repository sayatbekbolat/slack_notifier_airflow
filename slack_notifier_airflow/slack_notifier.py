from airflow.hooks.base_hook import BaseHook

from airflow.models import Variable
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


class Slack_Alert:
    def __init__(self, slack_conn_id, mention_users):
        self.message_template = '''
            status: Task {status}
            Dag: {dag}
            Task: {task}
            Execution time: {time}
            Logs : {logs_url}
        '''
        self.slack_conn_id = slack_conn_id
        self.slack_webhook_token = BaseHook.get_connection(self.slack_conn_id).password
        self.channel = BaseHook.get_connection(self.slack_conn_id).login
        self.mention_users = mention_users

    def run_alert(self, status, msg, context):
        slack_alert = SlackWebhookOperator(
            task_id=status,
            webhook_token=self.slack_webhook_token,
            message=msg,
            channel=self.channel,
            username='airflow_notifier',
            http_conn_id=self.slack_conn_id
        )
        return slack_alert.execute(context=context)

    def construct_mentioning(self):
        mention_str = str()
        for mention_user in self.mention_users:
            mention_str = mention_str + f'<@{mention_user}> '
        return mention_str

    def slack_fail_alert(self, context):
        msg = ":red_circle:" + self.construct_mentioning() + self.message_template.format(
            status='failed',
            dag=context.get('task_instance').dag_id,
            task=context.get('task_instance').task_id,
            time=context.get('execution_date'),
            logs_url=context.get('task_instance').log_url
        )
        return self.run_alert('failed', msg, context)

    def slack_retry_alert(self, context):
        msg = ":large_orange_circle:" + self.message_template.format(
            status='retried',
            dag=context.get('task_instance').dag_id,
            task=context.get('task_instance').task_id,
            time=context.get('execution_date'),
            logs_url=context.get('task_instance').log_url
        )
        return self.run_alert('retried_task', msg, context)

    def slack_dag_end_alert(self, **context):
        msg = ":large_green_circle:" + self.message_template.format(
            status='success',
            dag=context.get('task_instance').dag_id,
            time=context.get('execution_date'),
            logs_url=context.get('task_instance').log_url
        )
        return self.run_alert('success', msg, context)
