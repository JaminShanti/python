import yaml
import os
import pandas as pd
import yagmail
import datetime
from tabulate import tabulate
import html2text


email_titles = ['Checking up with you.']
logSpace = '  '


class ContactBot(object):
    def __init__(self):
        self.bot_file_exists = os.path.isfile('bot_file.yaml')
        self.verbose = True
        if self.bot_file_exists:
            with open('bot_file.yaml', 'r') as f:
                self.df = pd.io.json.json_normalize(yaml.load(f))
        else:
            self.df = pd.DataFrame()
        self.config_file_exists = os.path.isfile('contact_config.yaml')
        if self.config_file_exists:
            with open('contact_config.yaml', 'r') as ymlfile:
                self.cfg = yaml.safe_load(ymlfile)
        else:
            print "WARN: no config file found."

    def logprint(self, msg, level):
        if self.verbose:
            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                  [:-3] + logSpace + level + logSpace + msg)
        elif level != 'DEBUG':
            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                  [:-3] + logSpace + level + logSpace + msg)

    def add(self):
        keep_adding = True
        sentence_list = []
        contact_type_choice = None
        print "Contact Types: "
        for key,value in self.cfg['contact_types'].iteritems():
            print key
        contact_type_choice = raw_input('Enter the contact type: ')
        while contact_type_choice not in self.cfg['contact_types']:
            print "Invalid choice, please try again."
            contact_type_choice = raw_input('Enter the contact type: ')
        contact_type = self.cfg['contact_types'][contact_type_choice]
        full_name = raw_input('Enter the Contact\'s Name: ')
        first_name, last_name = full_name.split(' ')
        email_address = raw_input(
            'Enter the Contact\'s Email Address: ')
        specialty=raw_input("What specialty for the %s? " % contact_type_choice)
        while keep_adding:
            special_sentence=raw_input(
                'Enter a Familiar Sentence, enter blank to end: ')
            if special_sentence == '':
                keep_adding=False
            else:
                sentence_list.append(special_sentence)
        self.logprint("Pad extra sentence_list", "DEBUG")
        for i in range(len(sentence_list), 5):
            sentence_list.append('  ')
        bother_bot_delay=14
        last_contact_date=None
        self.df=self.df.append({"full_name": full_name, "first_name": first_name,
                                  "last_name": last_name, "contact_type": contact_type_choice, "specialty": specialty,
                                  "email_address": email_address, "sentence_list": sentence_list,
                                  "bother_bot_delay": bother_bot_delay, "last_contact_date": last_contact_date,
                                  "email_template_type": contact_type['email_template']}, ignore_index = True)

    def display(self):
        print tabulate(self.df[['full_name', 'contact_type', 'specialty']], headers = 'keys', tablefmt = 'psql')

    def save(self):
        self.logprint("Saving bot_file.yaml!", "INFO")
        with open('bot_file.yaml', 'w') as file:
            yaml.dump(self.df.to_dict(orient='records'),
                      file, default_flow_style = False)

    def reload(self):
        self.bot_file_exists=os.path.isfile('bot_file.yaml')
        if self.bot_file_exists:
            with open('bot_file.yaml', 'r') as f:
                self.df=pd.io.json.json_normalize(yaml.load(f))
        else:
            print "Error: file bot_file.yaml not found"

    def send_single_email(self, row):
        name=row['first_name'].encode("utf-8")
        recipient=row['email_address'].encode("utf-8")
        specialty=row['specialty'].encode("utf-8")
        self.logprint("Opening File: %s" %
                      row['email_template_type'].encode("utf-8"), "INFO")
        with open(row['email_template_type'].encode("utf-8")) as f:
            email_template=f.read()
        self.logprint("first_name is %s, specialty is %s, sentence_list is %s" % (
            name, specialty, row['sentence_list']), "DEBUG")
        html=email_template.format(first_name = name, specialty = specialty, special_sentence_1 = row['sentence_list'][0], special_sentence_2 = row['sentence_list'][
                                     1], special_sentence_3 = row['sentence_list'][2], special_sentence_4 = row['sentence_list'][3], special_sentence_5 = row['sentence_list'][4])
        self.logprint("Email Content: \n%s" %
                      html2text.html2text(html), "DEBUG")
        yag=yagmail.SMTP(self.cfg['Email_Account'],
                           self.cfg['Email_Password'])
        yag.send(to = recipient,
                 subject=email_titles[0], contents=html)

    def update_row(self, row):
        self.logprint("Reviewing Record: %s" % row['full_name'], "INFO")
        if row['last_contact_date'] == None:
            self.logprint("Sending Email...", "INFO")
            self.send_single_email(row)
            row['last_contact_date'] = datetime.datetime.now().strftime("%B %d, %Y")
        late_contact_date = datetime.datetime.strptime(
            row['last_contact_date'], "%B %d, %Y")
        diff_days = (datetime.datetime.now() - late_contact_date).days
        self.logprint("Time since last contact: %s days" % diff_days, "DEBUG")
        self.logprint("bother_bot_delay set to : %s" %
                      row['bother_bot_delay'], "DEBUG")
        if int(diff_days) > int(row['bother_bot_delay']):
            self.logprint("Sending Email...", "INFO")
            self.send_single_email(row)
            row['last_contact_date'] = datetime.datetime.now().strftime("%B %d, %Y")
        else:
            self.logprint(
                "Skipping Email based on bother_bot_delay...", "INFO")
        return row

    def run(self):
        self.df = self.df.apply(self.update_row, axis=1)
        self.save()

    def run_sample(self, name='Sample Example'):
        self.df[self.df.full_name == name] = self.df[self.df.full_name == name].apply(
            self.update_row, axis=1)
