import requests
import json
import datetime
import sqlite3
from typing import Optional, Dict, List
import argparse
import schedule
import time
import logging
from getpass import getpass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sms_sender.log'),
        logging.StreamHandler()
    ]
)

class SMSGateway:
    """Base class for SMS gateway implementations"""
    
    def __init__(self, api_key: str, sender_id: Optional[str] = None):
        self.api_key = api_key
        self.sender_id = sender_id
    
    def send_sms(self, recipient: str, message: str) -> Dict:
        """Send SMS to recipient"""
        raise NotImplementedError("Subclasses must implement this method")
    
    def get_balance(self) -> float:
        """Check account balance"""
        raise NotImplementedError("Subclasses must implement this method")
    
    def get_delivery_status(self, message_id: str) -> Dict:
        """Check delivery status of a message"""
        raise NotImplementedError("Subclasses must implement this method")


class TwilioGateway(SMSGateway):
    """Twilio SMS gateway implementation"""
    
    BASE_URL = "https://api.twilio.com/2010-04-01"
    
    def __init__(self, account_sid: str, auth_token: str, sender_id: Optional[str] = None):
        super().__init__(auth_token, sender_id)
        self.account_sid = account_sid
    
    def send_sms(self, recipient: str, message: str) -> Dict:
        url = f"{self.BASE_URL}/Accounts/{self.account_sid}/Messages.json"
        data = {
            "To": recipient,
            "Body": message,
            "From": self.sender_id if self.sender_id else self.account_sid
        }
        
        response = requests.post(
            url,
            data=data,
            auth=(self.account_sid, self.api_key)
        )
        
        if response.status_code == 201:
            return response.json()
        else:
            raise Exception(f"Twilio API error: {response.text}")
    
    def get_balance(self) -> float:
        url = f"{self.BASE_URL}/Accounts/{self.account_sid}/Balance.json"
        response = requests.get(url, auth=(self.account_sid, self.api_key))
        
        if response.status_code == 200:
            data = response.json()
            return float(data['balance'])
        else:
            raise Exception(f"Twilio API error: {response.text}")
    
    def get_delivery_status(self, message_id: str) -> Dict:
        url = f"{self.BASE_URL}/Accounts/{self.account_sid}/Messages/{message_id}.json"
        response = requests.get(url, auth=(self.account_sid, self.api_key))
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Twilio API error: {response.text}")


class PlivoGateway(SMSGateway):
    """Plivo SMS gateway implementation"""
    
    BASE_URL = "https://api.plivo.com/v1/Account/{account_id}"
    
    def __init__(self, auth_id: str, auth_token: str, sender_id: Optional[str] = None):
        super().__init__(auth_token, sender_id)
        self.auth_id = auth_id
    
    def send_sms(self, recipient: str, message: str) -> Dict:
        url = f"{self.BASE_URL.format(account_id=self.auth_id)}/Message/"
        data = {
            "src": self.sender_id if self.sender_id else self.auth_id,
            "dst": recipient,
            "text": message
        }
        
        response = requests.post(
            url,
            json=data,
            auth=(self.auth_id, self.api_key)
        )
        
        if response.status_code == 202:
            return response.json()
        else:
            raise Exception(f"Plivo API error: {response.text}")
    
    def get_balance(self) -> float:
        url = f"{self.BASE_URL.format(account_id=self.auth_id)}/"
        response = requests.get(url, auth=(self.auth_id, self.api_key))
        
        if response.status_code == 200:
            data = response.json()
            return float(data['cash_credits'])
        else:
            raise Exception(f"Plivo API error: {response.text}")
    
    def get_delivery_status(self, message_id: str) -> Dict:
        url = f"{self.BASE_URL.format(account_id=self.auth_id)}/Message/{message_id}/"
        response = requests.get(url, auth=(self.auth_id, self.api_key))
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Plivo API error: {response.text}")


class SMSDatabase:
    """Database handler for storing messages, contacts, and logs"""
    
    def __init__(self, db_path: str = "sms_messenger.db"):
        self.conn = sqlite3.connect(db_path)
        self._create_tables()
    
    def _create_tables(self):
        cursor = self.conn.cursor()
        
        # Create contacts table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT NOT NULL UNIQUE,
            email TEXT,
            group_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Create messages table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id INTEGER,
            recipient TEXT NOT NULL,
            message TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            gateway TEXT,
            message_id TEXT,
            scheduled_at TIMESTAMP,
            sent_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (contact_id) REFERENCES contacts (id)
        )
        """)
        
        # Create logs table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER,
            status TEXT,
            details TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (message_id) REFERENCES messages (id)
        )
        """)
        
        self.conn.commit()
    
    def add_contact(self, name: str, phone: str, email: Optional[str] = None, group: Optional[str] = None):
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO contacts (name, phone, email, group_name) VALUES (?, ?, ?, ?)",
                (name, phone, email, group)
            )
            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            raise ValueError(f"Contact with phone {phone} already exists")
    
    def get_contacts(self, group: Optional[str] = None) -> List[Dict]:
        cursor = self.conn.cursor()
        if group:
            cursor.execute("SELECT * FROM contacts WHERE group_name = ?", (group,))
        else:
            cursor.execute("SELECT * FROM contacts")
        
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def log_message(self, recipient: str, message: str, gateway: str, 
                   status: str = "pending", scheduled_at: Optional[datetime.datetime] = None,
                   contact_id: Optional[int] = None) -> int:
        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT INTO messages 
            (contact_id, recipient, message, status, gateway, scheduled_at) 
            VALUES (?, ?, ?, ?, ?, ?)""",
            (contact_id, recipient, message, status, gateway, scheduled_at)
        )
        self.conn.commit()
        return cursor.lastrowid
    
    def update_message_status(self, message_id: int, status: str, gateway_message_id: Optional[str] = None):
        cursor = self.conn.cursor()
        if gateway_message_id:
            cursor.execute(
                "UPDATE messages SET status = ?, message_id = ?, sent_at = CURRENT_TIMESTAMP WHERE id = ?",
                (status, gateway_message_id, message_id)
            )
        else:
            cursor.execute(
                "UPDATE messages SET status = ? WHERE id = ?",
                (status, message_id)
            )
        self.conn.commit()
    
    def add_log_entry(self, message_id: int, status: str, details: str):
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO logs (message_id, status, details) VALUES (?, ?, ?)",
            (message_id, status, details)
        )
        self.conn.commit()
    
    def get_pending_messages(self) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT * FROM messages 
        WHERE status = 'pending' 
        AND (scheduled_at IS NULL OR scheduled_at <= datetime('now'))
        """)
        
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def close(self):
        self.conn.close()


class SMSMessenger:
    """Main SMS messaging application"""
    
    def __init__(self):
        self.db = SMSDatabase()
        self.gateways = {}
        self.setup_logging()
    
    def setup_logging(self):
        self.logger = logging.getLogger('SMSMessenger')
    
    def add_gateway(self, name: str, gateway: SMSGateway):
        """Add an SMS gateway to the available gateways"""
        self.gateways[name] = gateway
        self.logger.info(f"Added gateway: {name}")
    
    def send_immediate_sms(self, gateway_name: str, recipient: str, message: str) -> Dict:
        """Send an SMS immediately"""
        if gateway_name not in self.gateways:
            raise ValueError(f"Gateway {gateway_name} not configured")
        
        gateway = self.gateways[gateway_name]
        
        try:
            # Log message in database
            db_message_id = self.db.log_message(
                recipient=recipient,
                message=message,
                gateway=gateway_name
            )
            
            # Send via gateway
            result = gateway.send_sms(recipient, message)
            
            # Update status in database
            self.db.update_message_status(
                message_id=db_message_id,
                status="sent",
                gateway_message_id=result.get('sid') or result.get('message_uuid')
            )
            
            # Add log entry
            self.db.add_log_entry(
                message_id=db_message_id,
                status="sent",
                details=json.dumps(result)
            )
            
            self.logger.info(f"Message sent to {recipient} via {gateway_name}")
            return result
        
        except Exception as e:
            self.logger.error(f"Failed to send message to {recipient}: {str(e)}")
            
            if db_message_id:
                self.db.update_message_status(db_message_id, "failed")
                self.db.add_log_entry(
                    message_id=db_message_id,
                    status="failed",
                    details=str(e)
                )
            
            raise
    
    def schedule_sms(self, gateway_name: str, recipient: str, message: str, 
                    schedule_time: datetime.datetime) -> int:
        """Schedule an SMS for future delivery"""
        if gateway_name not in self.gateways:
            raise ValueError(f"Gateway {gateway_name} not configured")
        
        db_message_id = self.db.log_message(
            recipient=recipient,
            message=message,
            gateway=gateway_name,
            status="scheduled",
            scheduled_at=schedule_time
        )
        
        self.logger.info(f"Message scheduled for {schedule_time} to {recipient}")
        return db_message_id
    
    def process_scheduled_messages(self):
        """Process all pending scheduled messages"""
        pending_messages = self.db.get_pending_messages()
        
        for message in pending_messages:
            gateway_name = message['gateway']
            recipient = message['recipient']
            message_text = message['message']
            db_message_id = message['id']
            
            if gateway_name not in self.gateways:
                self.logger.error(f"Gateway {gateway_name} not configured for message ID {db_message_id}")
                continue
            
            gateway = self.gateways[gateway_name]
            
            try:
                result = gateway.send_sms(recipient, message_text)
                
                self.db.update_message_status(
                    message_id=db_message_id,
                    status="sent",
                    gateway_message_id=result.get('sid') or result.get('message_uuid')
                )
                
                self.db.add_log_entry(
                    message_id=db_message_id,
                    status="sent",
                    details=json.dumps(result)
                )
                
                self.logger.info(f"Scheduled message sent to {recipient}")
            
            except Exception as e:
                self.logger.error(f"Failed to send scheduled message to {recipient}: {str(e)}")
                self.db.update_message_status(db_message_id, "failed")
                self.db.add_log_entry(
                    message_id=db_message_id,
                    status="failed",
                    details=str(e)
                )
    
    def check_delivery_status(self, message_id: str, gateway_name: str) -> Dict:
        """Check delivery status of a message"""
        if gateway_name not in self.gateways:
            raise ValueError(f"Gateway {gateway_name} not configured")
        
        gateway = self.gateways[gateway_name]
        return gateway.get_delivery_status(message_id)
    
    def get_gateway_balance(self, gateway_name: str) -> float:
        """Check balance for a gateway"""
        if gateway_name not in self.gateways:
            raise ValueError(f"Gateway {gateway_name} not configured")
        
        gateway = self.gateways[gateway_name]
        return gateway.get_balance()
    
    def add_contact(self, name: str, phone: str, email: Optional[str] = None, group: Optional[str] = None):
        """Add a new contact to the database"""
        return self.db.add_contact(name, phone, email, group)
    
    def get_contacts(self, group: Optional[str] = None) -> List[Dict]:
        """Get list of contacts, optionally filtered by group"""
        return self.db.get_contacts(group)
    
    def run_scheduler(self):
        """Run the message scheduler in a loop"""
        self.logger.info("Starting SMS scheduler...")
        schedule.every(1).minutes.do(self.process_scheduled_messages)
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Stopping SMS scheduler...")
    
    def close(self):
        """Clean up resources"""
        self.db.close()


def main():
    parser = argparse.ArgumentParser(description="SMS Messenger CLI")
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    # Send command
    send_parser = subparsers.add_parser('send', help='Send an SMS')
    send_parser.add_argument('--gateway', required=True, help='Gateway to use (twilio, plivo)')
    send_parser.add_argument('--to', required=True, help='Recipient phone number')
    send_parser.add_argument('--message', required=True, help='Message text')
    send_parser.add_argument('--schedule', help='Schedule time (YYYY-MM-DD HH:MM)')
    
    # Contacts command
    contacts_parser = subparsers.add_parser('contacts', help='Manage contacts')
    contacts_subparsers = contacts_parser.add_subparsers(dest='contacts_command', required=True)
    
    # Add contact
    add_contact_parser = contacts_subparsers.add_parser('add', help='Add a contact')
    add_contact_parser.add_argument('--name', required=True, help='Contact name')
    add_contact_parser.add_argument('--phone', required=True, help='Contact phone')
    add_contact_parser.add_argument('--email', help='Contact email')
    add_contact_parser.add_argument('--group', help='Contact group')
    
    # List contacts
    list_contacts_parser = contacts_subparsers.add_parser('list', help='List contacts')
    list_contacts_parser.add_argument('--group', help='Filter by group')
    
    # Gateway commands
    gateway_parser = subparsers.add_parser('gateway', help='Gateway operations')
    gateway_parser.add_argument('--name', required=True, help='Gateway name')
    gateway_subparsers = gateway_parser.add_subparsers(dest='gateway_command', required=True)
    
    # Check balance
    balance_parser = gateway_subparsers.add_parser('balance', help='Check gateway balance')
    
    # Setup gateway
    setup_parser = gateway_subparsers.add_parser('setup', help='Setup a gateway')
    setup_parser.add_argument('--type', required=True, choices=['twilio', 'plivo'], help='Gateway type')
    setup_parser.add_argument('--account-id', required=True, help='Account ID/SID')
    setup_parser.add_argument('--auth-token', help='Auth token (will prompt if not provided)')
    setup_parser.add_argument('--sender-id', help='Sender ID/phone number')
    
    # Scheduler command
    subparsers.add_parser('scheduler', help='Run the message scheduler')
    
    args = parser.parse_args()
    
    messenger = SMSMessenger()
    
    try:
        if args.command == 'send':
            # For demo purposes, we'll use environment variables or prompt for credentials
            # In a real app, you'd want to securely store these
            
            if args.gateway == 'twilio':
                account_sid = input("Enter Twilio Account SID: ")
                auth_token = getpass("Enter Twilio Auth Token: ")
                sender_id = input("Enter Twilio Sender ID (optional, press Enter to skip): ") or None
                
                gateway = TwilioGateway(account_sid, auth_token, sender_id)
                messenger.add_gateway('twilio', gateway)
            
            elif args.gateway == 'plivo':
                auth_id = input("Enter Plivo Auth ID: ")
                auth_token = getpass("Enter Plivo Auth Token: ")
                sender_id = input("Enter Plivo Sender ID (optional, press Enter to skip): ") or None
                
                gateway = PlivoGateway(auth_id, auth_token, sender_id)
                messenger.add_gateway('plivo', gateway)
            
            if args.schedule:
                schedule_time = datetime.datetime.strptime(args.schedule, '%Y-%m-%d %H:%M')
                messenger.schedule_sms(args.gateway, args.to, args.message, schedule_time)
                print(f"Message scheduled for {schedule_time}")
            else:
                result = messenger.send_immediate_sms(args.gateway, args.to, args.message)
                print("Message sent successfully:", result)
        
        elif args.command == 'contacts':
            if args.contacts_command == 'add':
                contact_id = messenger.add_contact(args.name, args.phone, args.email, args.group)
                print(f"Contact added with ID: {contact_id}")
            elif args.contacts_command == 'list':
                contacts = messenger.get_contacts(args.group)
                for contact in contacts:
                    print(f"{contact['id']}: {contact['name']} - {contact['phone']} ({contact['group_name'] or 'no group'})")
        
        elif args.command == 'gateway':
            if args.gateway_command == 'balance':
                # Similar gateway setup as in 'send' command
                if args.name == 'twilio':
                    account_sid = input("Enter Twilio Account SID: ")
                    auth_token = getpass("Enter Twilio Auth Token: ")
                    gateway = TwilioGateway(account_sid, auth_token)
                elif args.name == 'plivo':
                    auth_id = input("Enter Plivo Auth ID: ")
                    auth_token = getpass("Enter Plivo Auth Token: ")
                    gateway = PlivoGateway(auth_id, auth_token)
                
                messenger.add_gateway(args.name, gateway)
                balance = messenger.get_gateway_balance(args.name)
                print(f"Current balance for {args.name}: {balance}")
            
            elif args.gateway_command == 'setup':
                # This would normally save the credentials to a secure configuration
                print(f"Gateway {args.name} setup complete (demo - credentials not saved)")
        
        elif args.command == 'scheduler':
            print("Running scheduler. Press Ctrl+C to stop.")
            messenger.run_scheduler()
    
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        messenger.close()


if __name__ == "__main__":
    main()