import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import jinja2


html_template = 'html_status.jinja'
plaintext_template = 'plaintext_status.jinja'
base_url = 'http://uft21.ooi.rutgers.edu:12571'  # config


class EmailNotifier(object):
    def __init__(self, server):
        self.conn = smtplib.SMTP(server)
        loader = jinja2.PackageLoader('ooi_status', 'templates')
        self.env = jinja2.Environment(loader=loader, trim_blocks=True)

    def send_status(self, sender, receivers, subject, status_dict):
        html = self.apply_template(status_dict, html_template, 'html')
        text = self.apply_template(status_dict, plaintext_template, 'plain')

        message = MIMEMultipart('alternative')
        message['Subject'] = subject
        message['From'] = sender
        message['To'] = ', '.join(receivers)
        message.attach(text)
        message.attach(html)

        self.conn.sendmail(sender, receivers, message.as_string())

    def apply_template(self, status_dict, template, mimetype):
        text = self.env.get_template(template).render(status_dict=status_dict, base_url=base_url)
        return MIMEText(text, mimetype)