"""
File name: WebhookSender.py
Author: Martin Hein (@heimarti)
Date created: 08/03/2021
Date last modified: 10/03/2022
Version: 1.3
Python Version: 3.9
"""
import time
import requests


class WebhookSender:
    def send_webhook(self, url: str, message: str, retry: int):
        """
        The function handles the sending of Chime messages.
        :param url: String representation of the target Chime room's URL
        :param message: String representation of the message that it to be sent.
        :param retry: Integer specifying how many times should the function retry to send the message before quitting.
        :return: Boolean indicating whether the send was successful or not
        """
        successful = False
        try:
            data = {"Content": message}
            response = requests.post(url, json=data)
            if response.status_code == 200:
                successful = True
            elif retry > 0:
                retry -= 1
                time.sleep(0.3)
                self.send_webhook(url, message, retry)
            return successful
        except Exception as e:
            print("\nFailed to send Chime message: ", e)
            return successful


if __name__ == '__main__':
    webhook_sender = WebhookSender()
    # url can be found from the Chime chatroom, under Manage webhooks and bots. Get url by "copy URL"
    # message is custom.
    # number of retries is custom
    # Replace parameters but leave the quotation marks intact.
    webhook_sender.send_webhook(url='URL here', message='Message HERE', retry=5)
