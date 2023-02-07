import time
from datetime import datetime
import requests
import json
import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import plotly as py
import plotly.figure_factory as ff
import plotly.graph_objects as go
import plotly.express as px


## Static variables
MAX_PAGE = 20000

stacks = {
    "US": "https://connection.keboola.com/v2/storage/events",
    "EU-N": "https://connection.north-europe.azure.keboola.com/v2/storage/events",
    "EU-C": "https://connection.eu-central-1.keboola.com/v2/storage/events"
    }

guide = """
    ### Job Selector
    _1) Insert storage token_   
    _2) Get runid from the job event - usually there will be something like `810824168.810824226`, so runId is `810824168`_
    """

def grab_events(URL):
    """
    Download data via contacting Keboola storage API
    """
    section_log.text("Grabbing list of events from API")
    data_response = []
    raw_data = pd.DataFrame(data_response)
    params["offset"] = 0

    while params["offset"]<MAX_PAGE:
        r = requests.get(URL, headers=headers, params = params)
        #section_log.text(params["offset"])
        #section_log.text(r.url)
        try:
            raw_data = raw_data.append(pd.DataFrame(json.loads(r.text)), ignore_index = True)
            #section_log.write(r.status_code)
            section_log.info(r.url)
            message = ("API call: "+str(params["offset"]))
            section_log.info(message)
            #section_log.info(r.status_code)
        except:
            section_log.error(r.text)
            section_log.error(r.url)
            # end query
            params["offset"] = MAX_PAGE
        if len(json.loads(r.text))==0:
            params["offset"] = MAX_PAGE
        params["offset"] = params["offset"]+params["limit"]
    
    return(raw_data)



## Main
section_main = st.container()
section_main.title("Job debugger")


## Setup
section_setup = st.sidebar.container()
#section_setup.header("Job selector")

section_setup.markdown(guide)

stack = section_setup.radio("Select stack: ",["US", "EU-C", "EU-N"])
token = section_setup.text_input('Storage Token', type="password")
job_id = section_setup.text_input('Job ID')

section_setup.slider(
    'Number of event logs (default=5000)',
    0,
    MAX_PAGE,
    value=5000,
    step=100
    )


if section_setup.button("Gather events"):
    settings = {
        "stack": stack,
        "token": token,
        "job_id": job_id,
        "URL": stacks[stack]
        }

    headers = {
        'X-StorageApi-Token': settings["token"],
        "Accept": "application/json"}
    params = {
        "runId": settings["job_id"],
        "limit": 100,
        "offset": 0
    }
    
    section_main.text("Selected job: "+settings["job_id"])
    section_main.text("Contacting URL: "+settings["URL"])

    section_main.subheader("Contacting API...")

    ## LOG section
    section_log = section_main.empty()

    ## Run function to get data, assign it to dataframe
    data = grab_events(settings["URL"])

    ## Initial stats
    section_main.subheader("Acquired:")
    col_events, col_tasks, col_empty = section_main.columns(3)
    col_events.metric(label="EVENTS", value=len(data))
    col_tasks.metric(label="TASKS", value=len(data["runId"].unique()))

    data["runId"] = data["runId"].astype(str)

    conditions = [
        (data["message"].str.contains("Orchestration job \w+ start", na=False, regex=True)),
        (data["message"].str.contains("Orchestration job \w+ end", na=False, regex=True)),
        (data["message"].str.contains("Orchestration job \w+ scheduled", na=False, regex=True)),
        (data["message"].str.contains("Component *", na=False, regex=True)),
        (data["message"].str.contains("Job *", na=False, regex=True)),
        (data["message"].str.contains("Cloning ([1-9]|[1-9][0-9]|[1-9][0-9][0-9]|1000) \w+ to workspace", na=False, regex=True)),
        (data["component"].str.contains("storage", na=False, regex=True)),
        (data["message"].str.contains("Running component *", na=False, regex=True)),
        (data["message"].str.contains("Using component tag: *", na=False, regex=True))
        ]

    ## Perform mapping
    values = [
        "orchestration",
        "orchestration",
        "orchestration_state",
        "component_status",
        "job",
        "storage",
        "storage",
        "component", 
        "component_stat"
        ]
    data["event_hierarchy"] = np.select(conditions, values)
    
    ## Pick either to format hierarchy
    #data.loc[data["event_hierarchy"] == "0", "event_hierarchy"] = np.nan
    # or:
    def parse_component(column_input):
        """
        parse column values in the list form,
        use first one if only one, else use name from the rest
        """
        input = column_input.split("-")
        column_value = []

        if len(input)>1:
            column_value=input[1:]
        else:
            pass

        newlist = "_"
        column_value_new = newlist.join(column_value)

        return column_value_new

    
    data.loc[data["event_hierarchy"] == "0", "event_hierarchy"] = data["component"].apply(parse_component)

    data_timeline = data[["created","message", "runId", "component", "event_hierarchy"]]
    data_timeline = data_timeline.iloc[::-1].reset_index(drop = True)
    data_timeline["next_event"] = data_timeline["created"].shift(-1)

    ## Event list
    event_list = data_timeline["component"].unique()
    ## RunId list
    runId_list = data_timeline["runId"].unique()

    ## Time metrics
    start_time = data_timeline["created"].min()
    end_time = data_timeline["created"].max()
    
    start_time = datetime.strptime(start_time[:19], "%Y-%m-%dT%H:%M:%S")
    end_time = datetime.strptime(end_time[:19], "%Y-%m-%dT%H:%M:%S")
    
    duration = str(end_time-start_time)

    start = start_time.strftime("%H:%M:%S")
    end = end_time.strftime("%H:%M:%S")

    col1, col2, col3 = section_main.columns(3)
    col1.metric(label="Job START", value=start)
    col2.metric(label="Job END", value=end)
    col3.metric(label="Job DURATION", value=duration)

    ## Analysis section
    
    section_main.subheader("Job Analysis:")
    ## LOG section
    section_analysislog = section_main.empty()

    ## experimental - get component for each runId to define what is going on
    filter_set = set(["docker",
                    #"orchestrator",
                    "storage"])

    def remove_if_not_substring(l1, l2):
        """
        cleanup strings
        """
        return [i for i in l1 if not any(j in i for j in l2)]

    stages = {}
    for a in runId_list:
        runId_slice = data_timeline[data_timeline.runId == a]
        unique_components = runId_slice["component"].unique()
        components = list(unique_components)
        stages[a] = remove_if_not_substring(components, filter_set)[0]

    #section_main.text(stages)

    ## Assign stage
    data_timeline["stage"] = data_timeline["runId"].map(stages)

    ## duration in seconds
    try:
        data_timeline["duration"] = pd.to_timedelta(pd.to_datetime(data_timeline["next_event"])-pd.to_datetime(data_timeline["created"]),"sec",errors="ignore") #"coerce"
    except Exception as e:
        section_analysislog.error("could not parse datetime properly (duration)")
        #section_analysislog.error(e)

    data_timeline["duration"] = data_timeline["duration"].fillna(pd.Timedelta(seconds=0))

    try:
        data_timeline["duration"] = (data_timeline["duration"].astype(int, errors="ignore")).dt.seconds
    except Exception as e:
        section_analysislog.warning("could not parse datetime properly (cast seconds)")
        #section_analysislog.error(e)

    ## Data Preview
    @st.cache
    def convert_df(df):
        """
        get dataframe into cache
        """
        return df.to_csv().encode('utf-8')

    section_main.text("Data preview:")
    data_preview = pd.DataFrame(data_timeline,columns=[
        "created",
        "message",
        "runId",
        "component",
        "event_hierarchy",
        "next_event",
        "stage",
        "duration"
        ])
    data_preview["duration"] = data_preview["duration"] / np.timedelta64(1, 's')
    data_preview["duration"] = data_preview["duration"].astype(int, errors="ignore")

    section_main.dataframe(data_preview)

    ## Plot

    ## cutting the massage for the graph
    data_timeline["message"] = data_timeline["message"].str[:100]

    fig = px.timeline(
                    data_timeline,
                    x_start="created",
                    x_end="next_event",
                    y=data_timeline["stage"],
                    color = "component",
                    hover_name="component",
                    hover_data = [
                        "message",
                        "component",
                        "event_hierarchy",
                        "created",
                        "runId",
                        "stage",
                        "duration"
                        ],
                    width=1200,
                    height=800,
                    title=("Run ID: "+settings["job_id"])
                    )

    fig.update_xaxes(rangeslider_visible=True)
    fig.update_yaxes(autorange="reversed")

    ## Charting section
    section_chart = st.container()
    section_chart.title("Job Gantt Chart")
    section_chart.plotly_chart(fig, use_container_width=False, sharing="streamlit")

    ## Download Button
    csv = convert_df(data_preview)
    section_chart.download_button(
        "Download Dataset",
        csv,
        "file.csv",
        "text/csv",
        key="download-csv"
        )
    