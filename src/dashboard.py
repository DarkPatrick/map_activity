import streamlit as st
import os
import uuid
import plotly.express as px
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from datetime import datetime, timedelta, timezone
from time import sleep

from sql_worker import SqlWorker


class StreamlitApp:
    def __init__(self):
        if 'initialized' not in st.session_state:
            st.session_state['initialized'] = True
            self._sql_worker: SqlWorker = SqlWorker()
            self._content_generated = 0
            self.fig = None
            self.setup_session_state()

    def ensure_dir(self, file_path):
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

    def setup_session_state(self):
        if 'session_id' not in st.session_state:
            st.session_state['session_id'] = str(uuid.uuid4())

    def fetch_active_users(self):
        active_users = self._sql_worker.get_active_users()
        active_users.to_csv('active_users.csv')
        time_threshold = datetime.now(timezone.utc) - timedelta(minutes=1)
        active_users_filtered = active_users.loc[active_users['datetime'] >= time_threshold]
        return active_users_filtered, time_threshold

    def update_figure(self, active_users_filtered, time_threshold):
        # unique_sources = active_users_filtered['source'].unique()
        unique_sources = ['UG_WEB', 'UGT_IOS', 'UG_IOS', 'UGT_ANDROID', 'UG_ANDROID', 'UGT_HUAWEI']
        cmap = plt.cm.get_cmap('tab10', len(unique_sources))
        color_discrete_map = {}
        for i, source in enumerate(unique_sources):
            # color_discrete_map[source] = plt.colors.rgb2hex(cmap(i))
            color_discrete_map[source] = mcolors.rgb2hex(cmap(i))
        if self.fig is None:
            self.fig = px.scatter_geo(
                active_users_filtered,
                lat="latitude",
                lon="longitude",
                size="user_cnt",
                color="source",
                hover_name="region",
                hover_data=["user_cnt", "datetime"],
                opacity=active_users_filtered['datetime'].apply(
                    lambda x: max(0.1, (x - time_threshold).total_seconds() / 60)
                ),
                color_discrete_map=color_discrete_map,
                size_max=4,
            )
            self.fig.update_geos(fitbounds="locations")
        else:
            # trace_visibility = [trace.visible for trace in self.fig.data]
            # print(trace_visibility)
            # sleep(1)
            self.fig.data[0].lat = active_users_filtered['latitude']
            self.fig.data[0].lon = active_users_filtered['longitude']
            self.fig.data[0].marker.size = active_users_filtered['user_cnt']
            # self.fig.data[0].marker.color = active_users_filtered['source']
            self.fig.data[0].marker.opacity = active_users_filtered['datetime'].apply(
                lambda x: min(1, max(0.1, (x - time_threshold).total_seconds() / 60))
            )
            # self.fig.color_discrete_map=color_discrete_map,
            # self.fig.update_geos(fitbounds="locations")
            # for i, trace in enumerate(self.fig.data):
            #     trace.visible = trace_visibility[i] if i < len(trace_visibility) else True
            # for i, source in enumerate(active_users_filtered['source'].unique()):
            #     df_source = active_users_filtered[active_users_filtered['source'] == source]
            #     self.fig.data[i].lat = df_source['latitude']
            #     self.fig.data[i].lon = df_source['longitude']
            #     self.fig.data[i].marker.size = df_source['user_cnt']
            #     self.fig.data[i].marker.opacity = df_source['datetime'].apply(
            #         lambda x: min(1, max(0.1, (datetime.now(timezone.utc) - x).total_seconds() / 60))
            #     )

    # def display_active_users(self):
    #     while True:
    #         active_users = self._sql_worker.get_active_users()
    #         active_users.to_csv('active_users.csv')
    #         time_threshold = datetime.now(timezone.utc) - timedelta(minutes=1)
    #         active_users_filtered = active_users.loc[active_users['datetime'] >= time_threshold]
    #         fig = px.scatter_geo(
    #             active_users_filtered,
    #             # locations="region",
    #             lat="latitude",
    #             lon="longitude",
    #             # locationmode="country names",
    #             size="user_cnt",
    #             color="source",
    #             hover_name="region",
    #             hover_data=["user_cnt", "datetime"],
    #             # opacity='opacity',
    #             opacity=active_users_filtered['datetime'].apply(
    #                 lambda x: max(0.1, (x - time_threshold).total_seconds() / 60)
    #             ),
    #             size_max=4,
    #         )
    #         fig.update_geos(
    #             # projection_type="natural earth",
    #             fitbounds="locations",
    #         )
    #         # fig.update_layout(
    #         #     # autosize=True,
    #         #     # margin=dict(l=0, r=0, t=0, b=0),
    #         #     width=1000,
    #         # )
    #         st.title("User Activity Map")
    #         st.plotly_chart(fig, use_container_width=True)

    #         sleep(5)
    #         st.experimental_get_query_params()
    def display_active_users(self):
        placeholder = st.empty()
        while True:
            active_users_filtered, time_threshold = self.fetch_active_users()
            self.update_figure(active_users_filtered, time_threshold)
            # st.plotly_chart(self.fig, use_container_width=True)
            placeholder.plotly_chart(self.fig, use_container_width=True)
            sleep(1)

    def render(self):
        # st.title('Active users map')
        # st.title('')

        self.display_active_users()


if __name__ == '__main__':
    if 'app_instance' not in st.session_state:
        st.session_state['app_instance'] = StreamlitApp()
    st.session_state['app_instance'].render()
