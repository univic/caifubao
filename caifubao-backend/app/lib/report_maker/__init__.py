import logging
from app.lib.postoffice import post_office


class DailyReport(object):

    def __init__(self):
        self.subject = None
        self.msg_content = None
        self.msg_content_dict = {
            "": []
        }
        self.var1 = None
        self.para_template = f"""
        - {self.var1}\r\n
        {self.var1}\r\n 
        """
        pass

    def before_run(self):
        pass

    def compose_report(self, subject, msg_content_dict):
        self.subject = subject

    def send_report(self):
        post_office.send_mail()

    def set_subject(self, subject):
        self.subject = subject

    def add_content(self, section, content):
        if section in self.msg_content_dict:
            self.msg_content_dict[section].append(content)
        else:
            self.msg_content_dict[section] = content


daily_report_maker = DailyReport()
