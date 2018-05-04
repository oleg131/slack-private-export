import json
import requests
import os
import argparse
import pandas as pd

from slackclient import SlackClient
from pathlib2 import Path

pd.set_option('display.max_colwidth', -1)

parser = argparse.ArgumentParser()
parser.add_argument('--token', type=str, required=True,
    help='Get token here: https://api.slack.com/custom-integrations/legacy-tokens')
args = parser.parse_args()

token = args.token

path = Path('./export')

sc = SlackClient(token)

# Get user id to user name mapping
users = sc.api_call('users.list')
user_names = {i['id']: i['name'] for i in users['members']}

channels = sc.api_call('conversations.list')

# Get group id to group name mapping
groups = sc.api_call('groups.list')
group_names = {i['id']: i['name'] for i in groups['groups']}

# Get private conversations
ims = sc.api_call('im.list')
im_names = {i['id']: user_names[i['user']] for i in ims['ims']}

with open('users.txt', 'w') as f:
    json.dump(users, f)

with open('channels.txt', 'w') as f:
    json.dump(channels, f)

def get_msgs_by_id(call, id_):
    """Get all messages in channel."""

    msgs = []
    h = sc.api_call(call, channel=id_)
    msgs.extend(h['messages'])

    if not len(h['messages']):
        return msgs

    ts = h['messages'][-1]['ts']
    while h['has_more']:
        h = sc.api_call(call, channel=id_, latest=ts)
        msgs.extend(h['messages'])
        ts = h['messages'][-1]['ts']

    return msgs


def save_messages(p, messages):
    """Save messages as JSON and download all files mentioned."""

    pm = p / 'messages.json'
    with open(str(pm), 'w') as f:
        json.dump(messages, f)


def save_files(p, messages):
    files = []
    for m in messages:
        if 'file' in m:
            url = m['file']['url_private']
            pf = p / '{}-{}'.format(m['file']['id'], m['file']['name'])

            files.append('''{}
            header=Authorization: Bearer {}
            out={}'''.format(url, token, pf))

            # print('Downloading {}'.format(url))
            # os.system('wget --header="Authorization: Bearer {}" "{}" -O "{}"'.format(token, url, pf))

    return files


def save_html(p, messages):
    """Save messages as HTML."""

    if not len(messages):
        return

    df = pd.DataFrame(messages)
    df['ts'] = pd.to_datetime(df['ts'], unit='s')
    df['user_name'] = df['user'].replace(user_names)
    if 'file' in df:
        df['file_name'] = df['file'].apply(
            lambda x: 'FILE: {} / {}'.format(x['id'], x['name'])
            if not pd.isnull(x) else x)
    df = df.iloc[::-1]
    cols = ['ts', 'user_name', 'text', 'file_name']
    cols = [i for i in cols if i in df]
    html = df[cols].to_html(classes='table', index=False)
    header = '''
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="UTF-8">
        <link href="https://stackpath.bootstrapcdn.com/bootstrap/4.1.1/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-WskhaSGFgHYWDcbwN70/dfYBj47jz9qbsMId/iRN3ewGhXQFZCSftd1LZCfmhktB" crossorigin="anonymous">
      </head>
      <body>
        {}
      </body>
    </html>
    '''
    html = header.format(html)

    pm = p / 'conversation.html'
    with open(str(pm), 'w', encoding='utf') as f:
        f.write(html)

# Private channels
files = []
for g in groups['groups']:

    msgs = get_msgs_by_id('groups.history', g['id'])

    print('Processed channel', g['id'], g['name'], 'with', len(msgs),
        'messages')

    name = group_names[g['id']]
    p = path / name
    p.mkdir(exist_ok=True, parents=True)

    save_messages(p, msgs)
    save_html(p, msgs)
    f = save_files(p, msgs)
    files.extend(f)

# Direct messages
for im in ims['ims']:

    name = im_names[im['id']]

    msgs = get_msgs_by_id('im.history', im['id'])

    print('Processed IM', im['id'], name, 'with', len(msgs),
        'messages')

    p = path / name
    p.mkdir(exist_ok=True, parents=True)

    save_messages(p, msgs)
    save_html(p, msgs)
    f = save_files(p, msgs)
    files.extend(f)

print('Downloading all files')
pf = Path('.') / 'files.txt'
with open(str(pf), 'w', encoding='utf') as f:
    for i in files:
        f.write(i + '\n')
os.system('aria2c -i {} -l log.txt --log-level notice'.format(str(pf)))
