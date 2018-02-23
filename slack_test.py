#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import requests

webhook_url = 'XXXXXX'
slack_data = {'text': "Sup! We're hacking shit together <@maxim.gladkov> :spaghetti:. Проверка русского языка."}

response = requests.post(url=webhook_url, data=json.dumps(slack_data), headers={'Content-Type': 'application/json'})

if response.status_code != 200:
    raise ValueError(
        'Request to slack returned an error %s, the response is\n%s'
        % (response.status_code, response.text)
    )
else:
    print("Request is sent.\nReturn code: %s.\nReturn text: %s."
          % (response.status_code, response.text))

slack_data = {'text': 'This is a line of text in a channel <@inna>.\n' +
                      'And this is another line of text.\n' +
                      'Ссылка: <https://slack.com>\n' +
                      'Ссылка с текстом: <https://alert-system.com/alerts/1234|Click here>\n' +
                      'Вот такая проверка!',
              #'username': 'Changed name! Ooh!',
              #'icon_url': 'https://orientation.uga.edu/portals/3/Images/testing.png', # Или ссылка на иконку или эмоджи
              #'icon_emoji': ':ghost:',
              #'channel': '#reboot_testing',
              #'channel': '@maxim.gladkov',
              }

response = requests.post(url=webhook_url, data=json.dumps(slack_data), headers={'Content-Type': 'application/json'})
print("Request is sent.\nReturn code: %s.\nReturn text: %s."
      % (response.status_code, response.text))

slack_data = {'text': '*bold*\n`code`\n_italic_\n~strike~\n```pre```'
              }

#https://pythonworld.ru/osnovy/formatirovanie-strok-metod-format.html

response = requests.post(url=webhook_url, data=json.dumps(slack_data), headers={'Content-Type': 'application/json'})
print("Request is sent.\nReturn code: %s.\nReturn text: %s."
      % (response.status_code, response.text))

slack_data = {'text': '*bold*\n`code`\n_italic_\n~strike~\n```pre```',
              'mrkdwn': False
              }

response = requests.post(url=webhook_url, data=json.dumps(slack_data), headers={'Content-Type': 'application/json'})
print("Request is sent.\nReturn code: %s.\nReturn text: %s."
      % (response.status_code, response.text))

slack_data = {'attachments':
    [
        {'fallback': 'fallback: New open task [Urgent]: <http://url_to_task|Test out Slack message attachments>',
         'text': 'text: dgdggdffdfh\ndgdfgd\ngeggdd\n',
         #'pretext': 'pretext: Проверка названия',
         'color': '#D00000',
         'fields':
             [
                 {'title': 'fields:title: Notes',
                  'value': '```fields:value: This is much easier than I thought it would be.\nergegr\negerg\nererer```',
                  'short': True
                  }
             ]
         }
    ]
}
#https://api.slack.com/docs/message-attachments

response = requests.post(url=webhook_url, data=json.dumps(slack_data), headers={'Content-Type': 'application/json'})
print("Request is sent.\nReturn code: %s.\nReturn text: %s."
      % (response.status_code, response.text))

