# Delete empty containers on Flywheel
#   amf 07-2022
#
#       This is designed to be a routine.
#
#       Flywheel's DataView tool is used
#       to extract info at the subject, session, and
#       acquisition levels for a given project.
#
#       Missing values are used to identify and
#       delete empty containers at each level. Where empty containers are:
#           -- acquisitions with no files
#           -- sessions with no acquisitions
#           -- subjects with no sessions
#
#       Ignores any containers/files created in the last 24 hours
#       to avoid touching any current/simultaneous uploads
#

import flywheel
from datetime import datetime, timedelta
import os
import pandas as pd

fw = flywheel.Client(os.getenv("FLYWHEEL_API_TOKEN"))

sub_view = fw.View(
    container="subject",
    filename="*",
    match="all",
    columns=[
        "subject.created",
    ],
    include_ids=True,
    include_labels=True,
    process_files=False,
    sort=False,
)

ses_view = fw.View(
    container="session",
    filename="*",
    match="all",
    columns=[
        "session.created",
    ],
    include_ids=True,
    include_labels=True,
    process_files=False,
    sort=False,
)

file_view = fw.View(
    container="acquisition",
    filename="*",
    match="all",
    columns=[
        "file.name",
        "file.id",
        "file.created",
        "file.type"
    ],
    include_ids=True,
    include_labels=True,
    process_files=False,
    sort=False,
)

def delete_empty_containers(fw,df):
    for ind,row in df.iterrows():
        null_acq = 0
        null_ses = 0
        null_file = 0
        if pd.isna(row['acquisition.label']):
            null_acq = 1
        if pd.isna(row['session.label']):
            null_ses = 1
        if pd.isna(row['file.name']):
            null_file = 1
        if (null_ses == 1) and (null_acq==1): # empty subject
            fw.delete_subject(row['subject.id'])
        elif (null_ses == 0) and (null_acq==1): # empty session
            fw.delete_session(row['session.id'])
        elif (null_ses == 0) and (null_acq==0) and (null_file==1): # empty acquisition
            fw.delete_acquisition(row['acquisition.id'])


now = datetime.now()
last_day = now-timedelta(hours=24) # 24 hours ago
for project in fw.projects.iter():
    print(f'PROCESSING: {project.label}')
    # get all dataframes for this project
    project_contr = fw.projects.find_first('label='+project.label)
    sub_df = fw.read_view_dataframe(sub_view, project_contr.id)
    ses_df = fw.read_view_dataframe(ses_view, project_contr.id)
    file_df = fw.read_view_dataframe(file_view, project_contr.id)
    stop_processing = 0
    if (ses_df.empty) and (file_df.empty) and (sub_df.empty):
        stop_processing = 1
    elif (ses_df.empty) and (file_df.empty) and (not sub_df.empty):
        full_df = sub_df
    elif (not ses_df.empty) and (file_df.empty) and (not sub_df.empty):
        full_df = ses_df.merge(sub_df, how='outer')
    else:
        full_df = ses_df.merge(file_df, how='outer')
        full_df = full_df.merge(sub_df, how='outer')
    if stop_processing == 0:
        # ignore any rows for data created within the last 24 hours
        full_df = full_df[(pd.to_datetime(full_df['subject.created']).dt.tz_localize(None) < last_day) 
                            | (full_df['subject.created'].isnull())]
        full_df = full_df[(pd.to_datetime(full_df['session.created']).dt.tz_localize(None) < last_day) 
                            | (full_df['session.created'].isnull())]
        full_df = full_df[(pd.to_datetime(full_df['file.created']).dt.tz_localize(None) < last_day) 
                            | (full_df['file.created'].isnull())]
        # find and delete empty containers based on missing values
        empty_containers = full_df[full_df['file.name'].isnull()].drop_duplicates() # includes empty acquisitions, sessions, & subjects
        if not empty_containers.empty:
            print(f'    DELETING EMPTY CONTAINERS')
            delete_empty_containers(fw,empty_containers)
        else:
            print(f'    NO EMPTY CONTAINERS FOUND')


