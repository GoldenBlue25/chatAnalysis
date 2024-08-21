# Import necessary libraries
import json
import pandas as pd


class JsonConverter:
    """
    A class for handling JSON files containing Messenger group chat data and converting them into pandas DataFrames for data analysis.
    """

    path = ''           



    
    def __init__(self, path: str) -> None:
        '''
        Initializes the JsonConverter with the path to the JSON file.

        :param path: path to the JSON file
        '''

        self.path = path




    def make_participants_df(self) -> pd.DataFrame:        
        '''
        Creates a DataFrame summarizing participant activity in the chat.

        :return: A DataFrame with columns 'Messages', 'Words', 'Reactions_given', and 'Reactions_received'.
        '''

        # Temporary dictionary to store participant data
        participants_data = {}

        # Load json file
        with open(self.path) as f:
            data = json.load(f)
                
        # Add participants to the dictionary        
        for participant in data['participants']:
            participants_data.update({participant['name']: {'Messages': 0, 'Reactions_given': 0, 'Reactions_received': 0, 'Words': 0}})

        # Process messages to update participant data
        for message in data['messages']:
            try:
                participants_data[message['sender_name']]['Messages'] += 1
                participants_data[message['sender_name']]['Words'] += message['content'].count(" ") + 1
                participants_data[message['sender_name']]['Reactions_received'] += len(message['reactions'])
                for reaction in message['reactions']:
                    participants_data[reaction['actor']]['Reactions_given'] += 1      

            except(Exception):
                # Skip message if any required fields are missing
                continue

        # Create dataframe from the temporary dictionary
        summary_df = pd.DataFrame(columns = ['Messages', 'Words', 'Reactions_given', 'Reactions_received'])
        for participant in participants_data.items():
            summary_df.loc[bytes(participant[0], "latin1").decode("utf-8")] = {'Messages': participant[1]['Messages'], 'Words': participant[1]['Words'],'Reactions_given': participant[1]['Reactions_given'], 'Reactions_received': participant[1]['Reactions_received']}       

        return summary_df
    



    
    def make_messages_df(self) -> pd.DataFrame:
        '''
        Creates a DataFrame containing the timestamps and sender names for each message.

        :return: A DataFrame with columns 'Timestamp' and 'Name'.
        '''

        # Temporary dictionary to store message data
        messages_data = {}

        # Load json file
        with open(self.path) as f:
            data = json.load(f)

        # Process messages
        for message in data['messages']:
            try:
                messages_data.update({message['timestamp_ms'] : bytes(message['sender_name'], "latin1").decode("utf-8")})          
            except(Exception):
                # Skip message if any required fields are missing
                continue     
        
        # Create dataframe from the temporary dictionary
        message_df = pd.DataFrame(columns=['Timestamp', 'Name'])
        for message in messages_data.items():
            message_df.loc[len(message_df)] = {'Timestamp' : message[0], 'Name' : message[1]}

        return message_df    




    def make_reactions_df(self) -> pd.DataFrame:
        '''
        Creates a DataFrame with timestamps and actors for each reaction.

        :return: A DataFrame with columns 'Timestamp' and 'Name'.
        '''

        # Temporary dictionary to store reaction data
        reactions_data = {}

        # Load json file
        with open(self.path) as f:
            data = json.load(f)

        # Process reactions
        for message in data['messages']:
            try:
                for reaction in message['reactions']: 
                        reactions_data.update({len(reactions_data) : {'Timestamp' : message['timestamp_ms'], 'Name' : bytes(reaction['actor'], "latin1").decode("utf-8")}})        
            except(Exception):
                # Skip message if any required fields are missing
                continue   
               
        # Create dataframe from the temporary dictionary
        reactions_df = pd.DataFrame(columns=['Timestamp','Name'])
        for message in reactions_data.items():
            reactions_df.loc[len(reactions_df)] = {'Timestamp' : message[1]['Timestamp'], 'Name' : message[1]['Name']}

        return reactions_df   