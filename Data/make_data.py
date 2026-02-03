import pandas as pd
import numpy as np
import random
import string

n_users = 200000
n_follows = 20000000
n_activity = 200000

job_title = ['Software Engineer', 'Data Scientist', 'Product Manager', 'Marketing Manager', 'Sales Manager', 'HR Manager', 'Finance Manager', 'Legal Manager', 'IT Manager', 'Operations Manager']
actionType = ['like', 'comment', 'share', 'follow', 'unfollow', 'viewed', 'post', 'left a note', 'poked']
hobbies = [
    'Reading', 'Gardening', 'Photography', 'Cooking', 'Hiking', 'Painting',
    'Traveling', 'Swimming', 'Cycling', 'Running', 'Fishing', 'Knitting',
    'Dancing', 'Writing', 'Playing Guitar', 'Video Gaming', 'Bird Watching',
    'Woodworking', 'Sculpting', 'Chess', 'Origami', 'Baking', 'Surfing',
    'Rock Climbing', 'Pottery', 'Astronomy', 'Calligraphy', 'Martial Arts'
]

# Create all the dataframes with type constraints and load data into them

def getHobby():
    hobby = random.choice(hobbies)
    return hobby


circleNetPage = pd.DataFrame({
    "id": np.arange(1, n_users + 1),
    "nickname": [
        ''.join(random.choices(string.ascii_letters, k=random.randint(10, 20)))
        for _ in range(n_users)
    ],
    "job_title": np.random.choice(job_title, n_users),
    "region_code": np.random.randint(1, 51, n_users),
    "fav_hobby": [getHobby() for _ in range(n_users)]
})
print("Circle net page done")

follows = pd.DataFrame({
    "colRel": np.arange(1, n_follows + 1),
    "id1": np.random.randint(1, n_users + 1, n_follows),
    "id2": np.random.randint(1, n_users + 1, n_follows),
    "dateOfRel": pd.to_datetime(
        np.random.randint(1900, 2023, n_follows), format="%Y"
    ),
    "desc": [
        ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(20, 50)))
        for _ in range(n_follows)
    ]
})
print("follows done")

# This function ensures that viewed is always at the front of the array and there is a list of actions
def getAction():
    action = random.sample(actionType, random.randint(1, len(actionType) - 1))
    action = ["viewed"] + [x for x in action if x != "viewed"]
    return ",".join(action)


activityLog = pd.DataFrame({
    "actionID": np.arange(1, n_activity + 1),
    "byWho": np.random.randint(1, n_users + 1, n_activity),
    "page_id": np.random.randint(1, n_users + 1, n_activity),
    "actionType": [getAction() for _ in range(n_activity)],
    "actionTime": pd.to_datetime(np.random.randint(1900, 2023, n_activity), format="%Y")
})
print("activity log done")

#Export dataframes to csv files
circleNetPage.to_csv("circleNetPage.csv", index=False, header=False)
follows.to_csv("follows.csv", index=False, header=False, chunksize=100000)
activityLog.to_csv("activityLog.csv", index=False, header=False)