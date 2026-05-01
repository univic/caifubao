import os
import logging
import smtplib
import requests
from email import encoders
from email.header import Header
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from app.conf import app_config


logger = logging.getLogger(__name__)


class Messenger(object):
    def __init__(self):
        self.sender_email = app_config.MAIL_SENDER_EMAIL
        self.recipient_email_list = app_config.MAIL_RECIPIENT_EMAIL_LIST
        self.smtp_server_addr = app_config.MAIL_SMTP_SERVER_ADDR
        self.smtp_port = app_config.MAIL_SMTP_PORT
        self.smtp_username = app_config.MAIL_SMTP_USERNAME
        self.smtp_password = app_config.MAIL_SMTP_PASSWORD
        self.smtp_sender_display_name = app_config.MAIL_SMTP_SENDER_DISPLAY_NAME
        self.bark_url = app_config.BARK_URL

        self.server = None
        self.mail_msg = MIMEMultipart()

        self.mail_msg_from: str = ""
        self.mail_msg_to: str = ""
        self.mail_msg_subject: str = ""
        self.mail_msg_content = MIMEText("")
        self.mail_msg_html_content = MIMEText("")
        self.mail_msg_attachments_list: list = [
            {"path": "", "file_name": "", "mime": "", "file_extension": ""}
        ]

    def before_run(
        self,
        subject,
        msg_content,
        msg_html_content=None,
        recipient_email_list=None,
        attachment_list=None,
    ):
        self.mail_msg_subject = subject
        self.mail_msg_content = MIMEText(msg_content, "plain", "utf-8")
        self.mail_msg_html_content = MIMEText(msg_html_content, "html", "utf-8")
        if recipient_email_list:
            self.recipient_email_list = recipient_email_list

        if attachment_list:
            self.mail_msg_attachments_list = attachment_list
            self.handle_attachment_list()

    def handle_attachment_list(self):
        for path in self.mail_msg_attachments_list:
            # TODO: confirm path existence
            self.add_attachment(path)
        pass

    @staticmethod
    def extract_mime_info(path):
        mime_dict = {"txt": "text", "jpg": "image"}
        # filename with extension
        path = path.strip()
        file_name = os.path.basename(path)
        # get extension and remove dot
        extension = os.path.splitext(path)[-1][1:].lower()
        mime_type = mime_dict[extension]
        return file_name, extension, mime_type

    def connect_to_server(self):
        self.server = smtplib.SMTP(self.smtp_server_addr, self.smtp_port)
        self.server.starttls()
        self.server.set_debuglevel(1)
        self.server.login(self.smtp_username, self.smtp_password)

    def compose_msg(self):
        self.mail_msg["Subject"] = Header(self.mail_msg_subject)
        self.mail_msg["From"] = self.mail_msg_from
        self.mail_msg["To"] = self.mail_msg_to
        self.mail_msg.attach(self.mail_msg_content)
        self.mail_msg.attach(self.mail_msg_html_content)

    def add_attachment(self, path):
        file_name, file_extension, mime_type = self.extract_mime_info(path)
        with open(path, "rb") as f:
            mime = MIMEBase(mime_type, file_extension, file_name=file_name)
            mime.add_header("Content-Disposition", "attachment", filename=file_name)
            mime.add_header("Content-ID", "<0>")
            mime.add_header("X-Attachment-Id", "0")
            mime.set_payload(f.read())
            encoders.encode_base64(msg=mime)
            self.mail_msg.attach(mime)

    def send_mail(
        self,
        subject,
        msg_content,
        msg_html_content=None,
        recipient_email_list=None,
        attachment_path_list=None,
    ):
        self.before_run(
            subject,
            msg_content,
            msg_html_content,
            recipient_email_list,
            attachment_path_list,
        )
        self.connect_to_server()
        self.compose_msg()
        self.server.sendmail(
            self.sender_email, self.recipient_email_list, self.mail_msg.as_string()
        )
        self.after_run()

    def after_run(self):
        pass

    def send_bark_notification(self, title, content):
        payload = {
            "title": title,
            "body": content,
        }
        bark_response = requests.post(self.bark_url, json=payload)

        # 检查请求是否成功
        if bark_response.status_code == 200:
            logger.info(f"Successfully Sent Bark Notification: {payload}")
        else:
            logger.warning(f"Failed to send Bark Notification: {payload}")


messenger = Messenger()


if __name__ == "__main__":
    # msg_attachments_list: list = [{"path": "",
    #                                "file_name": "",
    #                                "mime": "",
    #                                "file_extension": ""}]
    # obj = PostOffice()
    # obj.send_mail()
    messenger.send_bark_notification(
        title="test message", content="test message from caifubao"
    )
    pass
