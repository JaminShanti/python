import yaml
import os
import pandas as pd
import yagmail
import datetime
import logging
from tabulate import tabulate
import html2text
# fbchat is often unstable or requires session cookies, keeping it optional
try:
    from fbchat import Client
    from fbchat.models import *
except ImportError:
    Client = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

EMAIL_TITLE_DEFAULT = 'Checking up with you.'

class ContactBot(object):
    def __init__(self, config_file='contact_config.yaml', bot_file='bot_file.yaml'):
        self.config_file = config_file
        self.bot_file = bot_file
        self.verbose = True
        self.bother_bot_delay = 20
        
        self.load_config()
        self.load_bot_file()

    def load_config(self):
        if os.path.isfile(self.config_file):
            with open(self.config_file, 'r') as ymlfile:
                self.cfg = yaml.safe_load(ymlfile)
            logger.info(f"Loaded configuration from {self.config_file}")
        else:
            logger.warning(f"No config file found at {self.config_file}")
            self.cfg = {}

    def load_bot_file(self):
        if os.path.isfile(self.bot_file):
            with open(self.bot_file, 'r') as f:
                data = yaml.safe_load(f)
                if data:
                    self.df = pd.json_normalize(data)
                else:
                    self.df = pd.DataFrame()
            logger.info(f"Loaded bot data from {self.bot_file}")
        else:
            logger.info(f"No bot file found at {self.bot_file}, starting with empty DataFrame.")
            self.df = pd.DataFrame()

    def add(self):
        if not self.cfg:
            logger.error("Configuration not loaded. Cannot add contact.")
            return

        print("Contact Types:")
        for key in self.cfg.get('contact_types', {}):
            print(key)
            
        contact_type_choice = input('Enter the contact type: ')
        while contact_type_choice not in self.cfg.get('contact_types', {}):
            print("Invalid choice, please try again.")
            contact_type_choice = input('Enter the contact type: ')
            
        contact_type = self.cfg['contact_types'][contact_type_choice]
        
        print("Notification Types:")
        for notif_type in self.cfg.get('notification_type', []):
            print(notif_type)
            
        notification_type_choice = input('Enter the notification type: ')
        while notification_type_choice not in self.cfg.get('notification_type', []):
            print("Invalid choice, please try again.")
            notification_type_choice = input('Enter the notification type: ')
            
        full_name = input('Enter the Contact\'s Name: ')
        try:
            first_name, last_name = full_name.split(' ', 1)
        except ValueError:
            first_name = full_name
            last_name = ""
            
        email_address = input('Enter the Contact\'s Email Address: ')
        specialty = input(f"What specialty for the {contact_type_choice}? ")
        facebook_id = input("What is their facebook ID? ")
        
        sentence_list = []
        while True:
            special_sentence = input('Enter a Familiar Sentence, enter blank to end: ')
            if special_sentence == '':
                break
            sentence_list.append(special_sentence)
            
        # Pad sentence list to ensure at least 5 elements
        while len(sentence_list) < 5:
            sentence_list.append('  ')
            
        new_row = {
            "full_name": full_name,
            "first_name": first_name,
            "last_name": last_name,
            "contact_type": contact_type_choice,
            "specialty": specialty,
            "email_address": email_address,
            "sentence_list": sentence_list,
            "bother_bot_delay": self.bother_bot_delay,
            "last_contact_date": None,
            "email_template_type": contact_type.get('email_template'),
            "notification_type": notification_type_choice,
            "facebook_id": facebook_id
        }
        
        # Use concat instead of append
        self.df = pd.concat([self.df, pd.DataFrame([new_row])], ignore_index=True)
        logger.info(f"Added contact: {full_name}")

    def display(self):
        if not self.df.empty:
            print(tabulate(self.df[['full_name', 'contact_type', 'specialty']], headers='keys', tablefmt='psql'))
        else:
            print("No records to display.")

    def save(self):
        logger.info(f"Saving to {self.bot_file}...")
        with open(self.bot_file, 'w') as file:
            # Convert DataFrame to list of dicts for YAML dumping
            yaml.dump(self.df.to_dict(orient='records'), file, default_flow_style=False)

    def reload(self):
        self.load_bot_file()

    def send_single_email(self, row):
        name = row['first_name']
        recipient = row['email_address']
        specialty = row['specialty']
        contact_type_key = row['contact_type']
        contact_type = self.cfg['contact_types'].get(contact_type_key, {})
        
        email_title = contact_type.get('email_titles', EMAIL_TITLE_DEFAULT)
        if not email_title:
            email_title = EMAIL_TITLE_DEFAULT
            
        template_file = row['email_template_type']
        logger.info(f"Opening Template File: {template_file}")
        
        try:
            with open(template_file, 'r') as f:
                email_template = f.read()
        except FileNotFoundError:
            logger.error(f"Template file {template_file} not found.")
            return

        logger.debug(f"Preparing email for {name}, specialty: {specialty}")
        
        # Safe access to sentence list
        sentences = row['sentence_list'] if isinstance(row['sentence_list'], list) else []
        while len(sentences) < 5:
            sentences.append('')
            
        html = email_template.format(
            first_name=name, 
            specialty=specialty, 
            special_sentence_1=sentences[0], 
            special_sentence_2=sentences[1], 
            special_sentence_3=sentences[2], 
            special_sentence_4=sentences[3], 
            special_sentence_5=sentences[4]
        )
        
        logger.debug(f"Email Subject: {email_title}")
        
        if row['notification_type'] == 'email':
            try:
                yag = yagmail.SMTP(self.cfg.get('Email_Account'), self.cfg.get('Email_Password'))
                yag.send(to=recipient, subject=email_title, contents=html)
                logger.info(f"Email sent to {recipient}")
            except Exception as e:
                logger.error(f"Failed to send email to {recipient}: {e}")
                
        elif row['notification_type'] == 'facebook':
            logger.info("Facebook notification skipped (feature disabled/unstable).")

    def process_row(self, row):
        logger.info(f"Reviewing Record: {row['full_name']}")
        
        should_send = False
        last_date_str = row.get('last_contact_date')
        
        if not last_date_str:
            should_send = True
        else:
            try:
                last_contact_date = datetime.datetime.strptime(last_date_str, "%B %d, %Y")
                diff_days = (datetime.datetime.now() - last_contact_date).days
                logger.debug(f"Time since last contact: {diff_days} days")
                
                delay = int(row.get('bother_bot_delay', self.bother_bot_delay))
                if diff_days > delay:
                    should_send = True
                else:
                    logger.info("Skipping contact based on delay.")
            except ValueError:
                logger.warning(f"Invalid date format for {row['full_name']}: {last_date_str}. Resetting.")
                should_send = True

        if should_send:
            logger.info("Sending notification...")
            self.send_single_email(row)
            row['last_contact_date'] = datetime.datetime.now().strftime("%B %d, %Y")
            
        return row

    def run(self):
        if not self.df.empty:
            self.df = self.df.apply(self.process_row, axis=1)
            self.save()
        else:
            logger.warning("No data to process.")

    def run_sample(self, name='Sample Example'):
        if not self.df.empty:
            mask = self.df['full_name'] == name
            if mask.any():
                self.df.loc[mask] = self.df.loc[mask].apply(self.process_row, axis=1)
                self.save()
            else:
                logger.warning(f"No record found with name: {name}")
