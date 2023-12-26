import json
import pymongo
from pymongo import MongoClient
from pymongo import errors
import sys
from pymongo.collation import Collation
from datetime import datetime
import time
import re

def ComposeTweet(content):
    global client

    try:
        # Connect to the '291db' database and 'tweets' collection
        db = client['291db']
        collection = db['tweets']
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return

    # Compose the tweet object with the system's date and time
    tweet = {
        "url": None,
        "date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
        "content": content,
        "renderedContent": None,
        "id": None,
        "user": {
            "username": "291user",
            "displayname": None,
            "id": None,
            "description": None,
            "rawDescription": None,
            "descriptionUrls": None,
            "verified": None,
            "created": None,
            "followersCount": None,
            "friendsCount": None,
            "statusesCount": None,
            "favouritesCount": None,
            "listedCount": None,
            "mediaCount": None,
            "location": None,
            "protected": None,
            "linkUrl": None,
            "linkTcourl": None,
            "profileImageUrl": None,
            "profileBannerUrl": None,
            "url": None
        },
        "outlinks": None,
        "tcooutlinks": None,
        "replyCount": None,
        "retweetCount": None,
        "likeCount": None,
        "quoteCount": None,
        "conversationId": None,
        "lang": None,
        "source": None,
        "sourceUrl": None,
        "sourceLabel": None,
        "media": None,
        "retweetedTweet": None,
        "quotedTweet": None,
        "mentionedUsers": None
}

    # Insert the tweet into the 'tweets' collection
    try:
        collection.insert_one(tweet)
        print("Tweet successfully composed and inserted into the database.")
    except Exception as e:
        print(f"Error inserting tweet into MongoDB: {e}")

def TopTweets(MAXI, type):
    global client

    try:
        # Connect to the '291db' database and 'tweets' collection
        db = client['291db']
        collection = db['tweets']
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return

    # Define the sort order based on the selected type
    sort_order = [(type, pymongo.DESCENDING)]

    # Use find to get the top MAXI tweets based on the selected field
    result = list(collection.find().sort(sort_order).limit(MAXI))

    # Display information for each matching tweet
     # Display information for each matching tweet
    print(f"\nTop Tweets based on {type}:")
    for tweet in result:
        print("\nTweet Information:")
        print(f"Tweet ID: {tweet['id']}")
        print(f"Date: {tweet['date']}")
        print(f"Content: {tweet['content']}")
        print(f"Username: {tweet['user']['username']}")
    # Prompt the user to select a tweet by index
    

    

        # Prompt the user for additional details about the user
    print("\nDo you want to Select a Tweet?")
    checking_while_fourth_q = True
    while checking_while_fourth_q:
        answer = input('Y/N: ').capitalize()
        output_user_prompt = ['Y', 'N']
        if answer in output_user_prompt:
            checking_while_fourth_q = False
        else:
            print('Invalid response. Please read the prompt. \n')

    if answer == 'N':
        return
    else:
        loop_break = True
        while loop_break:
            for i, tweet in enumerate(result):
                print(f'{i}  {tweet["id"]}')

            choose_user = input("Select a Tweet (Enter the number on left): ")

            # Check if the input is a valid integer
            if not choose_user.isdigit():
                print("Please enter a valid number.")
                return

            choose_user = int(choose_user)

            # Check if the chosen index is within the valid range
            if 0 <= choose_user < len(result):
                selected_tweet_id = result[choose_user]['id']
                selected_tweet = collection.find_one({"id": selected_tweet_id})

                # Display full information about the selected user
                if selected_tweet:
                    print("\nSelected Tweet:")
                    for field, value in selected_tweet.items():
                        print(f"{field}: {value}")
                    return
                else:
                    print(f"No user found with tid {result[choose_user]}")
                    return
            else:
                print("Please enter a valid number within the range.")
    

def TopUsers(MAXI, type):
    global client

    try:
        # Connect to the '291db' database and 'tweets' collection
        db = client['291db']
        collection = db['tweets']
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return

    # Define the sort order based on the selected type
    sort_order = [("user.followersCount", pymongo.DESCENDING)]

    # Use aggregation to group and project the data
    pipeline = [
        {"$sort": {"user.followersCount": pymongo.DESCENDING}},
        {"$group": {
            "_id": "$user.username",
            "user": {"$first": "$user"},
            "max_followers": {"$max": "$user.followersCount"}
        }},
        {"$sort": {"max_followers": pymongo.DESCENDING}},
        {"$limit": MAXI},
        {"$project": {
            "_id": 0,
            "username": "$_id",
            "user": 1
        }}
    ]

    result = list(collection.aggregate(pipeline))

    print(f"\nTop Users based on {type}:")
    for user in result:
        print("User Information:")
        print(f"Username: {user['username']}")
        print(f"Display Name: {user['user']['displayname']}")
        print(f"Followers Count: {user['user']['followersCount']}")
        print('\n')

    print("Do you want to check more about the users? \n")
    collection.create_index([("followersCount", pymongo.DESCENDING)])
    checking_while_fourth_q = True
    while checking_while_fourth_q:
        answer = input('Y/N: ').capitalize()
        output_user_prompt = ['Y', 'N']
        if answer in output_user_prompt:
            checking_while_fourth_q = False
        else:
            print('Invalid response. Please read the prompt. \n')

    if answer == 'N':
        return
    else:
        loop_break = True
        while loop_break:
            for i, user in enumerate(result):
                print(f'{i}  {user["username"]}')

            choose_user = input("Select a User (Enter the number): ")

            # Check if the input is a valid integer
            if not choose_user.isdigit():
                print("Please enter a valid number.")
                return

            choose_user = int(choose_user)

            # Check if the chosen index is within the valid range
            if 0 <= choose_user < len(result):
                selected_user = collection.find_one({"user.username": result[choose_user]["username"]})

                # Display full information about the selected user
                if selected_user:
                    print("\nSelected User:")
                    for field, value in selected_user.get("user", {}).items():
                        print(f"{field}: {value}")
                    return
                else:
                    print(f"No user found with ID {selected_user}")
                    return
            else:
                print("Please enter a valid number within the range.")






def SearchForUsers():
    global client

    try:
        # Connect to the '291db' database and 'tweets' collection
        db = client['291db']
        collection = db['tweets']
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return

    # Get keyword from the user
    keyword_input = str(input("Enter a keyword to search for users: "))

    # Debugging output
    print(f"Debug: Keyword Input: {keyword_input}")

    # Input validation
    keyword_input = keyword_input.strip()
    if not keyword_input:
        print("Please enter a non-empty keyword.")
        return

    # Make the keyword case-insensitive
    keyword = {"$regex": fr"\b{re.escape(keyword_input)}\b", "$options": "i"}


    # Construct a query to find tweets where user's displayname or location contains the keyword
    query = {
        "$or": [
            {"user.displayname": keyword},
            {"user.location": keyword},
        ]
    }

    try:
        # Use distinct to get unique values
        result = collection.distinct("user.username", filter=query)
    except Exception as e:
        print(f"Error querying the database: {e}")
        return

    users = []
    names = []
    for username in result:
        # Use query projection to fetch only the necessary fields
        user_info = collection.find_one({"user.username": username}, projection={"user": 1})
        users.append({
            "username": user_info["user"]["username"],
            "displayname": user_info["user"]["displayname"],
            "location": user_info["user"]["location"]
        })
        names.append(user_info["user"]["username"])

    # Display a list of matching users (username, displayname, and location)
    print("\nMatching Users:")
    for user in users:
        print(f'Username: {user["username"]}\nDisplay Name: {user["displayname"]}\nLocation: {user["location"]}\n\n\n')

    # Prompt the user to select a user by username
    selected_username = str(input("Enter the username of the user you want to view (or 'back' to go back to the main menu): "))

    # Check if the user wants to go back
    if selected_username.lower() == 'back':
        return

    # Find the selected user by username
    selected_user = collection.find_one({"user.username": selected_username})

    # Display full information about the selected user
    if selected_user and (selected_user["user"]["username"] in names):
        print("\nSelected User:")
        for field, value in selected_user["user"].items():
            print(f"{field}: {value}")
    else:
        print(f"No matching user found with username {selected_username}")

def SearchForTweets():
    global client

    try:
        # Connect to the '291db' database and 'tweets' collection
        db = client['291db']
        collection = db['tweets']
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return

    # Get keywords from the user
    keywords_input = str(input("Enter one or more keywords separated by spaces or punctuation: "))
    
    # Split the input into keywords using spaces and punctuation as delimiters
    # keywords = [re.escape(keyword.lower()) for keyword in re.split(r'[ \t\n.,;!?]', keywords_input)]
    # keywords = [re.escape(keyword.lower()) for keyword in re.split(r'[ \t\n.,;!?]+', keywords_input)]
    # keywords = [re.escape(keyword.lower()) for keyword in re.split(r'[ \t\n.,;!?]+', keywords_input)]
    keywords = [re.escape(keyword.lower()) for keyword in re.split(r'[ \t.,;!?]+|\n+', keywords_input)]

    # Construct a query to find tweets containing all of the keywords
    query = {
        "$and": [{"content": {"$regex": fr"\b{keyword}\b", "$options": "i"}} for keyword in keywords]
    }

    # Create a collation for case-insensitive matching
    collation = Collation(locale='en', strength=2)

    # Find matching tweets
    matching_tweets = collection.find(query).collation(collation)

    # Display a list of matching tweet IDs
    tweet_ids = []
    print("\nMatching Tweets:")
    for tweet in matching_tweets:
        print(f"ID: {tweet['id']}")
        print(f"Date: {tweet['date']}")
        print(f"Content: {tweet['content']}")
        print(f"Username: {tweet['user']['username']}")
        print("\n")
        tweet_ids.append(tweet['id'])

    intin = True
    while intin:
        select = input("Select a number:\n 1) Select a Tweet\n 2) Go Back\n")
        
        if select.isnumeric():
            select = int(select)
            intin = False
        else:
            print("Please enter an integer value")

    # Prompt the user to select a tweet by ID
    if select == 1:
        intin = True
        while intin:
            selected_tweet_id = input("Enter the ID of the tweet you want to view ('back' to go back to the main menu): ")
            if selected_tweet_id == 'back':
                return
            if selected_tweet_id.isnumeric():
                selected_tweet_id = int(selected_tweet_id)
                if selected_tweet_id in tweet_ids:
                    intin = False
                else:
                    print("Enter a search matching tweet id")
            else:
                print("Please enter an integer value")

        # Find the selected tweet by ID
        selected_tweet = collection.find_one({"id": int(selected_tweet_id)})

        # Display all fields of the selected tweet
        if selected_tweet:
            print("\nSelected Tweet:")
            for field, value in selected_tweet.items():
                print(f"{str(field)}: {str(value)}")
        else:
            print(f"No tweet found with ID {selected_tweet_id}")
    else:
        return



   


def load_json(json_file):
    global client
    if not json_file.lower().endswith('.json'):
        print(f"Error: {json_file} is not a valid JSON file.")
        sys.exit(1)

    try:
        db = client['291db']
        collection = db['tweets']

        if 'tweets' in db.list_collection_names():
            db.drop_collection('tweets')
            print("Dropped existing 'tweets' collection")

        batch_size = 1000
        tweets_batch = []

        with open(json_file, 'r') as file:
            for line_number, line in enumerate(file, start=1):
                try:
                    tweet = json.loads(line)
                    tweets_batch.append(tweet)

                    if len(tweets_batch) >= batch_size:
                        collection.insert_many(tweets_batch)
                        tweets_batch = []

                except json.JSONDecodeError as je:
                    print(f"Error decoding JSON on line {line_number}: {je}")
                except BulkWriteError as bwe:
                    print(f"Bulk write error on line {line_number}: {bwe.details}")

        if tweets_batch:
            collection.insert_many(tweets_batch)

        print("Data insertion completed.")

        # Create indexes for fields used in queries
        collection.create_index([("content", pymongo.TEXT)])
        collection.create_index([("user.username", pymongo.ASCENDING)])
        # Add more indexes as needed

    except FileNotFoundError:
        print(f"Error: File not found: {json_file}")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading JSON data into MongoDB: {e}")
        sys.exit(1)

    
def main():
    global client

    if len(sys.argv) != 3:
        print("Usage: python3 load_json.py <json_file> <mongodb_port>") #this order must be followed and is enforced by code
        sys.exit(1)

    json_file = str(sys.argv[1])
    port_str = sys.argv[2]

    if not port_str.isnumeric():
        print("Please enter a valid integer for the MongoDB port.")
        sys.exit(1)

    port = int(port_str)

    try:
        client = MongoClient('localhost', port, serverSelectionTimeoutMS=5000)
        # The following line is modified to actually test the connection
        client.server_info()
        print(f"Connected to MongoDB on port {port}")
    except errors.ServerSelectionTimeoutError:
        print(f"Error: Unable to connect to MongoDB. Please make sure MongoDB is running on port {port}.")
        sys.exit(1)
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        sys.exit(1)

    try:
        start_time = time.time()
        load_json(json_file)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Total execution time: {elapsed_time:.2f} seconds")

        secondary_menu_main = True
        while secondary_menu_main:
        
            intin = True
            while(intin):
                action = input('''Would you like to:\n 1) Search for a tweet\n 2) Search for a user\n 3) List Top (n) tweets\n 4) List Top (n) Users\n 5) Compose a Tweet\n 6) Exit Program\nPlease enter the number associated with the action: ''')
                if(action.isnumeric()):
                    action = int(action)
                    if action > 0 and action < 7:
                        intin = False
                    else:print("please enter 1-6")
                else:
                    print("Please enter an integer value")
            
            if action == 1:
                # new function needed to get followers tweets
                SearchForTweets()
                
                
            elif action == 2:
                SearchForUsers()
            elif action == 3:
                typing = ["retweetCount","likeCount","quoteCount"]
                intin = True
                while(intin):
                    n = input("How many Tweets do you want to see:")
                    if(n.isnumeric()):
                        n = int(n)
                        if n != 0:
                            intin = False
                        else:
                            print('0 is not a valid entrey')
                    else:
                        print("Please enter an integer value")
                
                
                print("pick (1-3) Tweets top in:")

                ho = 1
                for ty in typing:
                    print(f' {ho}    {ty}')
                    ho += 1

                intin = True
                while(intin):
                    i = input("please select a number on the left: ")
                    if(i.isnumeric()):
                        i = int(i)
                        if ((i > 0) and (i<4)):
                            
                            intin = False
                        else:
                            print('Please enter 1,2, or 3')
                    else:
                        print("Please enter an integer value")
                


                TopTweets(int(n),typing[int(i-1)])

            elif action == 4:
                typing = ["Followers Count"]

                
                intin = True
                while(intin):
                    i = input("How many Users do you want to see:")
                    if(i.isnumeric()):
                        i = int(i)
                        if i != 0:
                            intin = False
                    else:
                        print("Please enter a non-zero integer value")

                # ho = 0
                # for ty in typing:
                #     print(f' {ho}    {ty}')
                #     ho += 1
                # i = input("please select a number on the left: ")
                TopUsers(int(i),typing[0])
            
            elif action == 5:
                content = str(input("Please enter your Tweets Text:\n"))
                ComposeTweet(content) 
                #Compose a tweet The user should be able to compose a tweet by entering a tweet content. 
                # Your system should insert the tweet to the database, set the date filed to the system date and username to "291user". All other fields will be null.
                pass
            elif action == 6:
                secondary_menu_main=False
                client.close()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        # Close the MongoDB connection in case of an error
        client.close()
        sys.exit(1)
    finally:
        # Close the MongoDB connection in case of successful execution
        client.close()


if __name__ == "__main__":
    main()
#################### so we can connect to the port but if the port isnt open on mongodb it fails we should catch this error idk how