# pip3 install virtualenv
# python3 -m venv venv
# . ./venv/bin/activate
# pip install telethon pandas openpyxl
# pip install pyarrow  # For Parquet support
# pip install fastparquet  # For Parquet support
# python scrape.py

# Initial imports
from datetime import datetime, timezone
import pandas as pd
import time
import json
import re
import asyncio

# Telegram imports
from telethon.sync import TelegramClient

# Setup / change only the first time you use it
# @markdown **1.1.** Your Telegram account username (just 'abc123', not '@'):
username = 'XXX' # @param {type:"string"}
# @markdown **1.2.** Your Telegram account phone number (ex: '+5511999999999'):
phone = 'XXX' # @param {type:"string"}
# @markdown **1.3.** Your API ID, it can be only generated from https://my.telegram.org/apps:
api_id = 'XXX' # @param {type:"string"}
# @markdown **1.4.** Your API hash, also from https://my.telegram.org/apps:
api_hash = 'XXX' # @param {type:"string"}
channels = "@XXX" # @param {type:"string"}
channels = [channel.strip() for channel in channels.split(",")]

# @markdown **2.2.** Here you can select the `time window` you would like to extract data from the listed communities:
date_min = '2025-01-20' # @param {type:"date"}
date_max = '2025-04-20' # @param {type:"date"}

date_min = datetime.fromisoformat(date_min).replace(tzinfo=timezone.utc)
date_max = datetime.fromisoformat(date_max).replace(tzinfo=timezone.utc)

# @markdown **2.3.** Choose a `name` for the final file you want to download as output:
file_name = 'XXX' # @param {type:"string"}

# @markdown **2.4.** `Keyword` to search, **leave empty if you want to extract all messages from the channel(s):**
key_search = '' # @param {type:"string"}

# @markdown **2.5.** **Maximum** `number of messages` to scrape (only use if you want a specific limit, otherwise leave a high number to scrape everything):
max_t_index = 1000000   # @param {type:"integer"}

# @markdown **2.6.** `Timeout in seconds` (never leave it longer than 6 hours, that is 21600 seconds, as Google Colab deactivates itself after that time):
time_limit = 21600 # @param {type:"integer"}

# @markdown **2.7.** Choose the format of the final file you want to download. If you are a first-time user, choose `Excel`. If you have advanced skills, you can use `Parquet`:
File = 'parquet' # @param ["excel", "parquet"]

# @markdown **Attention:** During this step, Telegram may request a verification code. Please monitor your Telegram app and input the required information promptly. Rest assured, all data entered remains secure.

# Function to remove invalid XML characters from text
def remove_unsupported_characters(text):
    valid_xml_chars = (
        "[^\u0009\u000A\u000D\u0020-\uD7FF\uE000-\uFFFD"
        "\U00010000-\U0010FFFF]"
    )
    cleaned_text = re.sub(valid_xml_chars, '', text)
    return cleaned_text

# Function to format time in days, hours, minutes, and seconds
def format_time(seconds):
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f'{int(days):02}:{int(hours):02}:{int(minutes):02}:{int(seconds):02}'

# Function to print progress of the scraping process
def print_progress(t_index, message_id, start_time, max_t_index):
    elapsed_time = time.time() - start_time
    current_progress = t_index / (t_index + message_id) if (t_index + message_id) <= max_t_index else t_index / max_t_index
    percentage = current_progress * 100
    estimated_total_time = elapsed_time / current_progress
    remaining_time = estimated_total_time - elapsed_time

    elapsed_time_str = format_time(elapsed_time)
    remaining_time_str = format_time(remaining_time)

    print(f'Progress: {percentage:.2f}% | Elapsed Time: {elapsed_time_str} | Remaining Time: {remaining_time_str}')

async def scrape(file_format, channels, date_min, date_max, key_search, max_t_index, time_limit, t_index, start_time, data):
    # Normalize File variable to avoid issues
    file_format = re.sub(r'[^a-z]', '', file_format.lower())  # Converts to lowercase and removes non-alphabetic characters
    print(f'Username: {username}')
    print(f'Phone: {phone}')
    print(f'API ID: {api_id}')
    print(f'API Hash: {api_hash}')
    print(f'Channels: {channels}')
    print(f'File format: {file_format}')

    # Scraping process
    for channel in channels:
        print(f'\n\n{"-" * 50}\n#Scraping {channel}...\n{"-" * 50}\n')

        if t_index >= max_t_index:
            break

        if time.time() - start_time > time_limit:
            break

        loop_start_time = time.time()

        try:
            c_index = 0
            async with TelegramClient(username, api_id, api_hash) as client:
                async for message in client.iter_messages(channel, search=key_search):
                    try:
                        if date_min <= message.date <= date_max:

                            # Process comments of the message
                            comments_list = []
                            try:
                                async for comment_message in client.iter_messages(channel, reply_to=message.id):
                                    comment_text = comment_message.text.replace("'", '"')

                                    comment_media = 'True' if comment_message.media else 'False'

                                    comment_emoji_string = ''
                                    if comment_message.reactions:
                                        for reaction_count in comment_message.reactions.results:
                                            emoji = reaction_count.reaction.emoticon
                                            count = str(reaction_count.count)
                                            comment_emoji_string += emoji + " " + count + " "

                                    comment_date_time = comment_message.date.strftime('%Y-%m-%d %H:%M:%S')

                                    comments_list.append({
                                        'Type': 'comment',
                                        'Comment Group': channel,
                                        'Comment Author ID': comment_message.sender_id,
                                        'Comment Content': comment_text,
                                        'Comment Date': comment_date_time,
                                        'Comment Message ID': comment_message.id,
                                        'Comment Author': comment_message.post_author,
                                        'Comment Views': comment_message.views,
                                        'Comment Reactions': comment_emoji_string,
                                        'Comment Shares': comment_message.forwards,
                                        'Comment Media': comment_media,
                                        'Comment Url': f'https://t.me/{channel}/{message.id}?comment={comment_message.id}'.replace('@', ''),
                                    })
                            except Exception as e:
                                comments_list = []
                                print(f'Error processing comments: {e}')

                            # Process the main message
                            media = 'True' if message.media else 'False'

                            emoji_string = ''
                            if message.reactions:
                                for reaction_count in message.reactions.results:
                                    emoji = reaction_count.reaction.emoticon
                                    count = str(reaction_count.count)
                                    emoji_string += emoji + " " + count + " "

                            date_time = message.date.strftime('%Y-%m-%d %H:%M:%S')
                            cleaned_content = remove_unsupported_characters(message.text)
                            cleaned_comments_list = remove_unsupported_characters(json.dumps(comments_list))

                            data.append({
                                'Type': 'text',
                                'Group': channel,
                                'Author ID': message.sender_id,
                                'Content': cleaned_content,
                                'Date': date_time,
                                'Message ID': message.id,
                                'Author': message.post_author,
                                'Views': message.views,
                                'Reactions': emoji_string,
                                'Shares': message.forwards,
                                'Media': media,
                                'Url': f'https://t.me/{channel}/{message.id}'.replace('@', ''),
                                'Comments List': cleaned_comments_list,
                            })

                            c_index += 1
                            t_index += 1

                            # Print progress
                            print(f'{"-" * 80}')
                            print_progress(t_index, message.id, start_time, max_t_index)
                            current_max_id = min(c_index + message.id, max_t_index)
                            print(f'From {channel}: {c_index:05} contents of {current_max_id:05}')
                            print(f'Id: {message.id:05} / Date: {date_time}')
                            print(f'Total: {t_index:05} contents until now')
                            print(f'{"-" * 80}\n\n')

                            if t_index % 1000 == 0:
                                if file_format == 'parquet':
                                    backup_filename = f'backup_{file_name}_until_{t_index:05}_{channel}_ID{message.id:07}.parquet'
                                    pd.DataFrame(data).to_parquet(backup_filename, index=False)
                                elif file_format == 'excel':
                                    backup_filename = f'backup_{file_name}_until_{t_index:05}_{channel}_ID{message.id:07}.xlsx'
                                    pd.DataFrame(data).to_excel(backup_filename, index=False, engine='openpyxl')

                            if t_index >= max_t_index:
                                break

                            if time.time() - start_time > time_limit:
                                break

                        elif message.date < date_min:
                            break

                    except Exception as e:
                        print(f'Error processing message: {e}')

            print(f'\n\n##### {channel} was ok with {c_index:05} posts #####\n\n')

            df = pd.DataFrame(data)
            if file_format == 'parquet':
                partial_filename = f'complete_{channel}_in_{file_name}_until_{t_index:05}.parquet'
                df.to_parquet(partial_filename, index=False)
            elif file_format == 'excel':
                partial_filename = f'complete_{channel}_in_{file_name}_until_{t_index:05}.xlsx'
                df.to_excel(partial_filename, index=False, engine='openpyxl')
            # files.download(partial_filename)

        except Exception as e:
            print(f'{channel} error: {e}')

        loop_end_time = time.time()
        loop_duration = loop_end_time - loop_start_time

        if loop_duration < 60:
            time.sleep(60 - loop_duration)

    print(f'\n{"-" * 50}\n#Concluded! #{t_index:05} posts were scraped!\n{"-" * 50}\n\n\n\n')
    df = pd.DataFrame(data)
    if File == 'parquet':
        final_filename = f'FINAL_{file_name}_with_{t_index:05}.parquet'
        df.to_parquet(final_filename, index=False)
    elif File == 'excel':
        final_filename = f'FINAL_{file_name}_with_{t_index:05}.xlsx'
        df.to_excel(final_filename, index=False, engine='openpyxl')

data = []  # List to store scraped data
t_index = 0  # Tracker for the number of messages processed
start_time = time.time()  # Record the start time for the scraping session

if __name__ == "__main__":
    asyncio.run(scrape(File, channels, date_min, date_max, key_search, max_t_index, time_limit, t_index, start_time, data))