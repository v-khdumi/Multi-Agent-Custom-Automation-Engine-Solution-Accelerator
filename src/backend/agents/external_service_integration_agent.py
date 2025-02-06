import time
import random
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ExternalServiceIntegrationAgent")

class ExternalServiceIntegrationAgent:
    def __init__(self, erp_url, automation_url):
        self.erp_url = erp_url
        self.automation_url = automation_url

    def fetch_erp_data(self):
        """
        Simulate retrieving data from an ERP system.
        """
        logger.info(f"Fetching data from ERP system at {self.erp_url} ...")
        time.sleep(1)  # Simulate API delay
        # For demo purposes, we simulate ERP data as a list of record dictionaries.
        data = [
            {"id": 1001, "name": "Order A", "status": "Pending"},
            {"id": 1002, "name": "Order B", "status": "Pending"},
            {"id": 1003, "name": "Order C", "status": "Pending"}
        ]
        logger.info("Data retrieved from ERP system.")
        return data

    def post_data_to_automation(self, data):
        """
        Simulate posting data to the automation engine.
        """
        logger.info(f"Posting data to Automation Engine at {self.automation_url} ...")
        time.sleep(1)  # Simulate API delay
        # Simulate a success transfer by returning an updated version of the data.
        updated_data = []
        for record in data:
            record_copy = record.copy()
            # Simulate a status change after processing (e.g., 'Processed')
            record_copy['status'] = "Processed"
            updated_data.append(record_copy)
        logger.info("Data successfully synchronized with Automation Engine.")
        return updated_data

    def send_confirmation_notification(self, updated_data):
        """
        Simulate sending confirmation notifications (e.g., email or dashboard update).
        """
        logger.info("Sending confirmation notifications...")
        time.sleep(0.5)  # Brief delay for notification
        # For demo purposes, log confirmation and return a success message.
        notification_message = f"{len(updated_data)} records synchronized successfully."
        logger.info(f"Notification: {notification_message}")
        return notification_message

    def sync_data(self):
        """
        Workflow to synchronize ERP data with the automation system,
        and then send notifications upon confirmation.
        """
        logger.info("Initiating data synchronization process...")

        # Step 1: Initiation - a user request triggers the sync.
        erp_data = self.fetch_erp_data()

        # Step 2: Processing - the agent posts the ERP data to the automation engine.
        updated_data = self.post_data_to_automation(erp_data)

        # Simulate data consistency check.
        if self.data_consistency_check(erp_data, updated_data):
            # Step 3: Confirmation - send notification if data is synchronized correctly.
            confirmation_msg = self.send_confirmation_notification(updated_data)
            return confirmation_msg, updated_data
        else:
            logger.error("Data consistency check failed!")
            return "Data consistency check failed.", None

    def data_consistency_check(self, original, updated):
        """
        Verify that the number of records remains the same and each record is updated accordingly.
        """
        if len(original) != len(updated):
            return False
        for orig, updt in zip(original, updated):
            # Check if record IDs match and the status of updated record is 'Processed'
            if orig["id"] != updt["id"] or updt["status"] != "Processed":
                return False
        return True

if __name__ == "__main__":
    # Define dummy endpoint URLs for the demo.
    erp_api_url = "https://dummy-erp.example.com/api/data"
    automation_api_url = "https://automation-engine.example.com/api/sync"

    agent = ExternalServiceIntegrationAgent(erp_api_url, automation_api_url)
    confirmation, data = agent.sync_data()
    logger.info(f"Final confirmation: {confirmation}")
